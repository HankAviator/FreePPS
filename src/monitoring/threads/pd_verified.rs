use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use log::{debug, error, info, warn};

#[cfg(unix)]
use crate::common::constants::{
    BATTERY_STATUS_PATH, PD_VERIFIED_PATH, QCOM_ADAPTER_SVID_PATH, TYPEC_MODE_PATH,
    USB_ONLINE_PATH, USB_REAL_TYPE_PATH,
};
use crate::common::utils;
#[cfg(unix)]
use crate::monitoring::ChargingMode;
#[cfg(unix)]
use crate::monitoring::FileMonitor;
use crate::pd::PdVerifier;
#[cfg(unix)]
use crate::pd::{BroadcastForger, spawn_broadcast_forger_worker};
use crate::platform::EventFd;

const NATIVE_NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(4);
const PUBLIC_IDENTITY_SETTLE: Duration = Duration::from_millis(400);
const PUBLIC_IDENTITY_WINDOW: Duration = Duration::from_secs(1);
const PUBLIC_IDENTITY_RECHECK: Duration = Duration::from_millis(100);
const PUBLIC_REASSERT_DELAY: Duration = Duration::from_millis(500);
const PUBLIC_VERIFY_DELAY: Duration = Duration::from_millis(1500);
const DETACH_DEBOUNCE: Duration = Duration::from_millis(1500);
// A USB meter/adapter can keep Type-C attached while its upstream charger is
// swapped. The retry budget therefore belongs to the current upstream charger
// session, not to the physical Type-C attachment.
const MAX_PUBLIC_RETRIES_PER_UPSTREAM_SESSION: u8 = 2;

#[derive(Clone, Copy, Debug)]
enum AutoPhase {
    Idle,
    IdentifyingPublic {
        next_check: Instant,
        identity_deadline: Instant,
        native_deadline: Instant,
    },
    WaitingNative(Instant),
    EnablingPublic(Instant),
    VerifyingPublic(Instant),
    Settled,
}

impl AutoPhase {
    fn deadline(self) -> Option<Instant> {
        match self {
            Self::IdentifyingPublic { next_check, .. } => Some(next_check),
            Self::WaitingNative(deadline)
            | Self::EnablingPublic(deadline)
            | Self::VerifyingPublic(deadline) => Some(deadline),
            Self::Idle | Self::Settled => None,
        }
    }
}

#[cfg(any(unix, test))]
fn should_process_physical_detach(attached: bool, physically_attached: bool) -> bool {
    attached && !physically_attached
}

#[cfg(any(unix, test))]
fn public_pps_activation_succeeded(verified: &str, usb_type: &str) -> bool {
    verified == "1" && usb_type == "PD_PPS"
}

#[cfg(any(unix, test))]
fn should_enable_public_early(verified: &str, usb_type: &str, adapter_svid: &str) -> bool {
    verified != "1" && usb_type == "PD_PPS" && adapter_svid == "0000"
}

fn automatic_attach_phase(now: Instant) -> AutoPhase {
    AutoPhase::IdentifyingPublic {
        next_check: now + PUBLIC_IDENTITY_SETTLE,
        identity_deadline: now + PUBLIC_IDENTITY_WINDOW,
        native_deadline: now + NATIVE_NEGOTIATION_TIMEOUT,
    }
}

#[cfg(any(unix, test))]
fn should_rearm_public_retry(
    charger_detach_armed: bool,
    usb_detach_uevent_seen: bool,
    verified: &str,
    usb_type: &str,
    battery_status: &str,
    usb_online: &str,
) -> bool {
    charger_detach_armed
        && usb_detach_uevent_seen
        && verified == "0"
        && usb_type == "Unknown"
        && battery_status == "Discharging"
        && usb_online == "0"
}

#[cfg(any(unix, test))]
fn can_start_public_retry(attempt_count: u8) -> bool {
    attempt_count < MAX_PUBLIC_RETRIES_PER_UPSTREAM_SESSION
}

#[cfg(any(unix, test))]
fn should_start_native_wait_after_upstream_attach(
    upstream_native_pending: bool,
    battery_status: &str,
    usb_online: &str,
) -> bool {
    upstream_native_pending && battery_status == "Charging" && usb_online == "1"
}

#[cfg(any(unix, test))]
fn should_capture_upstream_detach(phase: AutoPhase, charger_detach_armed: bool) -> bool {
    matches!(phase, AutoPhase::Settled) && charger_detach_armed
}

pub fn spawn_pd_verified_monitor(
    running: Arc<AtomicBool>,
    pd_verifier: Arc<PdVerifier>,
    charging_mode: Arc<AtomicU8>,
    config_event: Arc<EventFd>,
    stop_event: Arc<EventFd>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("qcom".to_string())
        .spawn(move || {
            if let Err(error) = worker(
                running,
                pd_verifier,
                charging_mode,
                config_event,
                stop_event,
            ) {
                error!("qcom线程出错: {}", error);
            }
        })
        .expect("创建qcom线程失败")
}

fn worker(
    running: Arc<AtomicBool>,
    pd_verifier: Arc<PdVerifier>,
    charging_mode: Arc<AtomicU8>,
    config_event: Arc<EventFd>,
    stop_event: Arc<EventFd>,
) -> Result<()> {
    info!("[{}] 启动qcom监控线程...", utils::get_current_thread_name());

    #[cfg(unix)]
    run_unix(
        running,
        pd_verifier,
        charging_mode,
        config_event,
        stop_event,
    )?;

    #[cfg(not(unix))]
    let _ = (
        running,
        pd_verifier,
        charging_mode,
        config_event,
        stop_event,
    );

    Ok(())
}

#[cfg(unix)]
fn is_attached() -> Result<bool> {
    Ok(FileMonitor::read_file_content(TYPEC_MODE_PATH)? != "Nothing attached")
}

#[cfg(unix)]
fn epoll_timeout(
    phase: AutoPhase,
    detach_deadline: Option<Instant>,
    uevent_recheck_deadline: Option<Instant>,
    charger_detach_deadline: Option<Instant>,
) -> libc::c_int {
    let deadline = [
        phase.deadline(),
        detach_deadline,
        uevent_recheck_deadline,
        charger_detach_deadline,
    ]
    .into_iter()
    .flatten()
    .min();
    let Some(deadline) = deadline else {
        return -1;
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        0
    } else {
        remaining.as_millis().clamp(1, libc::c_int::MAX as u128) as libc::c_int
    }
}

#[cfg(unix)]
fn run_unix(
    running: Arc<AtomicBool>,
    pd_verifier: Arc<PdVerifier>,
    charging_mode: Arc<AtomicU8>,
    config_event: Arc<EventFd>,
    stop_event: Arc<EventFd>,
) -> Result<()> {
    let uevent_sock = FileMonitor::create_uevent_monitor()?;
    let event_monitor = FileMonitor::new()?;

    let mut mode = ChargingMode::from_raw(charging_mode.load(Ordering::Acquire));
    let mut uevent_registered = mode != ChargingMode::Native;
    let setup = event_monitor
        .add_fd_to_epoll(
            stop_event.raw_fd(),
            libc::EPOLLIN as u32,
            stop_event.raw_fd() as u64,
        )
        .and_then(|()| {
            event_monitor.add_fd_to_epoll(
                config_event.raw_fd(),
                libc::EPOLLIN as u32,
                config_event.raw_fd() as u64,
            )
        })
        .and_then(|()| {
            if uevent_registered {
                event_monitor.add_fd_to_epoll(
                    uevent_sock,
                    (libc::EPOLLIN | libc::EPOLLPRI) as u32,
                    uevent_sock as u64,
                )
            } else {
                Ok(())
            }
        });
    if let Err(error) = setup {
        unsafe { libc::close(uevent_sock) };
        return Err(error);
    }

    info!("通过uevent与短时协商定时器监控qcom状态");
    let mut attached = is_attached()?;
    let mut detach_deadline = None;
    let mut phase = match (mode, attached) {
        (ChargingMode::Automatic, true) => automatic_attach_phase(Instant::now()),
        _ => AutoPhase::Idle,
    };
    // Bound retries per upstream charger session. A second attempt is
    // available for a real charger swap or a failed first negotiation, but
    // transient USB/PD events cannot create an unbounded reconnect loop.
    let mut public_retry_count = 0;
    let mut public_retry_attempted = false;
    // An upstream swap must re-enter the Xiaomi-first negotiation window
    // before public PPS fallback is allowed. This is separate from
    // `public_retry_attempted`, which reserves the current session budget.
    let mut upstream_native_pending = false;
    let mut uevent_recheck_deadline = None;
    let mut charger_detach_armed = false;
    let mut usb_detach_uevent_seen = false;
    let mut charger_detach_deadline = None;
    // Preserve upstream's SystemUI gold-label/100 W broadcast feature. The
    // worker is only activated for a charging session and exits with the daemon.
    let session_gen = Arc::new(AtomicU32::new(0));
    let session_active = Arc::new(AtomicBool::new(false));
    let broadcast_session_event = Arc::new(EventFd::new()?);
    let broadcast_stop_event = Arc::new(EventFd::new()?);
    let broadcast_handle = spawn_broadcast_forger_worker(
        Arc::clone(&running),
        Arc::clone(&session_gen),
        Arc::clone(&session_active),
        Arc::clone(&broadcast_session_event),
        Arc::clone(&broadcast_stop_event),
        Arc::new(BroadcastForger),
    );
    let mut charging_session_active = false;
    if uevent_registered
        && attached
        && FileMonitor::read_file_content(BATTERY_STATUS_PATH).unwrap_or_default() == "Charging"
    {
        start_charging_session(
            &mut charging_session_active,
            &session_gen,
            &session_active,
            &broadcast_session_event,
        );
    }

    let mut events = [libc::epoll_event { events: 0, u64: 0 }; 8];

    while running.load(Ordering::Relaxed) {
        let nfds = match event_monitor.wait_events(
            &mut events,
            epoll_timeout(
                phase,
                detach_deadline,
                uevent_recheck_deadline,
                charger_detach_deadline,
            ),
        ) {
            Ok(count) => count,
            Err(error) => {
                if matches!(error.raw_os_error(), Some(code) if code == libc::EINTR || code == libc::EAGAIN)
                {
                    continue;
                }
                error!("qcom epoll_wait失败: {}", error);
                thread::sleep(Duration::from_secs(1));
                continue;
            }
        };

        let ready = &events[..nfds as usize];
        if ready
            .iter()
            .any(|event| event.u64 == stop_event.raw_fd() as u64)
        {
            stop_event.clear()?;
            break;
        }

        if ready
            .iter()
            .any(|event| event.u64 == config_event.raw_fd() as u64)
        {
            config_event.clear()?;
            let new_mode = ChargingMode::from_raw(charging_mode.load(Ordering::Acquire));
            let should_monitor_uevents = new_mode != ChargingMode::Native;
            if should_monitor_uevents != uevent_registered {
                if should_monitor_uevents {
                    event_monitor.add_fd_to_epoll(
                        uevent_sock,
                        (libc::EPOLLIN | libc::EPOLLPRI) as u32,
                        uevent_sock as u64,
                    )?;
                } else {
                    event_monitor.remove_fd_from_epoll(uevent_sock)?;
                }
                uevent_registered = should_monitor_uevents;
                info!("[qcom] uevent监控状态: {}", uevent_registered);
            }
            mode = new_mode;
            attached = is_attached()?;
            detach_deadline = None;
            public_retry_count = 0;
            public_retry_attempted = false;
            upstream_native_pending = false;
            charger_detach_armed = false;
            usb_detach_uevent_seen = false;
            charger_detach_deadline = None;
            uevent_recheck_deadline = None;
            phase = match (mode, attached) {
                (ChargingMode::Automatic, true) => automatic_attach_phase(Instant::now()),
                _ => AutoPhase::Idle,
            };
            if uevent_registered && attached {
                start_charging_session(
                    &mut charging_session_active,
                    &session_gen,
                    &session_active,
                    &broadcast_session_event,
                );
            } else {
                stop_charging_session(
                    &mut charging_session_active,
                    &session_active,
                    &broadcast_session_event,
                );
            }
            debug!("qcom收到模式变化: {:?}", mode);
        }

        if uevent_registered && ready.iter().any(|event| event.u64 == uevent_sock as u64) {
            // Drain every queued netlink datagram so epoll cannot spin on stale events.
            let capture_upstream_detach =
                should_capture_upstream_detach(phase, charger_detach_armed);
            let mut buffer = [0u8; 4096];
            loop {
                let bytes_read = unsafe {
                    libc::recv(
                        uevent_sock,
                        buffer.as_mut_ptr().cast::<libc::c_void>(),
                        buffer.len(),
                        libc::MSG_DONTWAIT,
                    )
                };
                if bytes_read <= 0 {
                    break;
                }

                if capture_upstream_detach {
                    let data = String::from_utf8_lossy(&buffer[..bytes_read as usize]);
                    let is_usb_power_supply = data
                        .split(['\0', '\n'])
                        .any(|field| field == "POWER_SUPPLY_NAME=usb");
                    let usb_online = data
                        .split(['\0', '\n'])
                        .find_map(|field| field.strip_prefix("POWER_SUPPLY_ONLINE="));
                    if is_usb_power_supply && usb_online == Some("0") {
                        usb_detach_uevent_seen = true;
                    }
                }
            }
            if capture_upstream_detach {
                // The first read can race the driver update; reconcile once
                // more after the event without polling while idle.
                uevent_recheck_deadline
                    .get_or_insert_with(|| Instant::now() + Duration::from_millis(250));
            }
        }
        if uevent_recheck_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            uevent_recheck_deadline = None;
        }
        let physically_attached = is_attached()?;
        if should_process_physical_detach(attached, physically_attached) {
            let deadline = detach_deadline.get_or_insert(Instant::now() + DETACH_DEBOUNCE);
            if Instant::now() >= *deadline {
                attached = false;
                detach_deadline = None;
                if mode == ChargingMode::Automatic {
                    pd_verifier.set_pd_verified(false)?;
                    info!("[自动] 已拔出，恢复小米协议优先基线");
                }
                public_retry_count = 0;
                public_retry_attempted = false;
                upstream_native_pending = false;
                charger_detach_armed = false;
                usb_detach_uevent_seen = false;
                charger_detach_deadline = None;
                uevent_recheck_deadline = None;
                stop_charging_session(
                    &mut charging_session_active,
                    &session_active,
                    &broadcast_session_event,
                );
                phase = AutoPhase::Idle;
            }
        } else if physically_attached {
            detach_deadline = None;
            if !attached {
                attached = true;
                public_retry_count = 0;
                public_retry_attempted = false;
                upstream_native_pending = false;
                charger_detach_armed = false;
                usb_detach_uevent_seen = false;
                charger_detach_deadline = None;
                uevent_recheck_deadline = None;
                if mode == ChargingMode::Automatic {
                    phase = automatic_attach_phase(Instant::now());
                    info!("[自动] 检测到连接，识别充电器并优先等待小米协议认证");
                }
                if mode != ChargingMode::Native {
                    start_charging_session(
                        &mut charging_session_active,
                        &session_gen,
                        &session_active,
                        &broadcast_session_event,
                    );
                }
            }
        }

        if mode != ChargingMode::Automatic || !attached {
            upstream_native_pending = false;
            charger_detach_armed = false;
            usb_detach_uevent_seen = false;
            charger_detach_deadline = None;
            uevent_recheck_deadline = None;
            continue;
        }

        // A USB meter can keep Type-C physically attached while its upstream
        // charger is unplugged. Arm this for any stable Xiaomi or public-PPS
        // session. This preserves charger-swap support without polling while
        // the charging state is stable.
        if matches!(phase, AutoPhase::Settled) {
            let verified = FileMonitor::read_file_content(PD_VERIFIED_PATH)?;
            let usb_type = FileMonitor::read_file_content(USB_REAL_TYPE_PATH)?;
            let battery_status =
                FileMonitor::read_file_content(BATTERY_STATUS_PATH).unwrap_or_default();
            let usb_online = FileMonitor::read_file_content(USB_ONLINE_PATH).unwrap_or_default();
            let retry_failed = public_retry_attempted
                && battery_status == "Charging"
                && usb_online == "1"
                && verified != "1"
                && usb_type == "PD_PPS"
                && !upstream_native_pending
                && can_start_public_retry(public_retry_count);

            if should_start_native_wait_after_upstream_attach(
                upstream_native_pending,
                &battery_status,
                &usb_online,
            ) {
                upstream_native_pending = false;
                charger_detach_armed = false;
                charger_detach_deadline = None;
                usb_detach_uevent_seen = false;
                phase = automatic_attach_phase(Instant::now());
                info!("[自动] 检测到新充电器，重新识别协议并优先等待小米认证");
            } else if battery_status == "Charging" && usb_online == "1" {
                if !charger_detach_armed {
                    debug!("[自动] 充电稳定，启用上游断开检测");
                    charger_detach_armed = true;
                    charger_detach_deadline = None;
                    usb_detach_uevent_seen = false;
                }
            } else if should_rearm_public_retry(
                charger_detach_armed,
                usb_detach_uevent_seen,
                &verified,
                &usb_type,
                &battery_status,
                &usb_online,
            ) {
                let deadline =
                    charger_detach_deadline.get_or_insert_with(|| Instant::now() + DETACH_DEBOUNCE);
                if Instant::now() >= *deadline {
                    // A stable offline interval is evidence that the upstream
                    // charger changed, so reserve a fresh bounded session and
                    // require the replacement charger to pass the Xiaomi-first
                    // window before allowing public PPS fallback.
                    public_retry_count = 0;
                    public_retry_attempted = true;
                    upstream_native_pending = true;
                    charger_detach_armed = false;
                    usb_detach_uevent_seen = false;
                    charger_detach_deadline = None;
                    info!("[自动] 检测到充电器已从转接设备断开，重置预算并等待小米协议认证");
                }
            } else {
                charger_detach_deadline = None;
                if usb_online != "0" {
                    usb_detach_uevent_seen = false;
                }
            }

            if retry_failed {
                public_retry_attempted = false;
                upstream_native_pending = false;
                charger_detach_armed = false;
                usb_detach_uevent_seen = false;
                charger_detach_deadline = None;
                info!("[自动] 公版PPS重连后验证状态未保持，允许一次受限重试");
            }
        }

        phase = match phase {
            AutoPhase::IdentifyingPublic {
                next_check,
                identity_deadline,
                native_deadline,
            } => {
                let verified = FileMonitor::read_file_content(PD_VERIFIED_PATH)?;
                if verified == "1" {
                    info!("[自动] 小米协议认证成功，保持原生协商");
                    AutoPhase::Settled
                } else if Instant::now() >= next_check {
                    let usb_type = FileMonitor::read_file_content(USB_REAL_TYPE_PATH)?;
                    let adapter_svid =
                        FileMonitor::read_file_content(QCOM_ADAPTER_SVID_PATH).unwrap_or_default();
                    if should_enable_public_early(&verified, &usb_type, &adapter_svid)
                        && can_start_public_retry(public_retry_count)
                    {
                        info!("[自动] 提前识别到公版PPS，立即启用以保留高功率协商窗口");
                        public_retry_count += 1;
                        public_retry_attempted = true;
                        upstream_native_pending = false;
                        pd_verifier.set_pd_verified(true)?;
                        AutoPhase::EnablingPublic(Instant::now() + PUBLIC_REASSERT_DELAY)
                    } else if !adapter_svid.is_empty() && adapter_svid != "0000" {
                        debug!("[自动] 检测到厂商SVID={}，继续等待小米认证", adapter_svid);
                        AutoPhase::WaitingNative(native_deadline)
                    } else if Instant::now() < identity_deadline {
                        AutoPhase::IdentifyingPublic {
                            next_check: (Instant::now() + PUBLIC_IDENTITY_RECHECK)
                                .min(identity_deadline),
                            identity_deadline,
                            native_deadline,
                        }
                    } else {
                        AutoPhase::WaitingNative(native_deadline)
                    }
                } else {
                    AutoPhase::IdentifyingPublic {
                        next_check,
                        identity_deadline,
                        native_deadline,
                    }
                }
            }
            AutoPhase::WaitingNative(deadline) => {
                if FileMonitor::read_file_content(PD_VERIFIED_PATH)? == "1" {
                    info!("[自动] 小米协议认证成功，保持原生协商");
                    AutoPhase::Settled
                } else if Instant::now() >= deadline {
                    let usb_type = FileMonitor::read_file_content(USB_REAL_TYPE_PATH)?;
                    if usb_type == "PD_PPS" && can_start_public_retry(public_retry_count) {
                        info!("[自动] 未检测到小米认证，直接启用公版PPS");
                        public_retry_count += 1;
                        public_retry_attempted = true;
                        upstream_native_pending = false;
                        pd_verifier.set_pd_verified(true)?;
                        AutoPhase::EnablingPublic(Instant::now() + PUBLIC_REASSERT_DELAY)
                    } else {
                        warn!("[自动] 未认证且接口类型为{}，本次不强制切换", usb_type);
                        AutoPhase::Settled
                    }
                } else {
                    AutoPhase::WaitingNative(deadline)
                }
            }
            AutoPhase::EnablingPublic(deadline) if Instant::now() >= deadline => {
                // Xiaomi's PD state machine can clear the first write while it
                // transitions into charge-pump mode. Reassert once inside the
                // measured high-power decision window, then verify later.
                pd_verifier.set_pd_verified(true)?;
                debug!("[自动] 在高功率协商窗口内再次确认公版PPS状态");
                AutoPhase::VerifyingPublic(Instant::now() + PUBLIC_VERIFY_DELAY)
            }
            AutoPhase::VerifyingPublic(deadline) if Instant::now() >= deadline => {
                let verified = FileMonitor::read_file_content(PD_VERIFIED_PATH)?;
                let usb_type = FileMonitor::read_file_content(USB_REAL_TYPE_PATH)?;
                if public_pps_activation_succeeded(&verified, &usb_type) {
                    info!("[自动] 公版PPS已启用并通过状态验证");
                } else {
                    warn!(
                        "[自动] 公版PPS启用验证失败: pd_verifed={}, usb_real_type={}",
                        verified, usb_type
                    );
                    if verified != "1"
                        && usb_type == "PD_PPS"
                        && can_start_public_retry(public_retry_count)
                    {
                        public_retry_attempted = false;
                    }
                }
                AutoPhase::Settled
            }
            AutoPhase::Settled if !public_retry_attempted => {
                let verified = FileMonitor::read_file_content(PD_VERIFIED_PATH)?;
                let usb_type = FileMonitor::read_file_content(USB_REAL_TYPE_PATH)?;
                if verified != "1"
                    && usb_type == "PD_PPS"
                    && can_start_public_retry(public_retry_count)
                {
                    info!("[自动] 已连接设备后检测到公版PPS，直接启用公版PPS");
                    public_retry_count += 1;
                    public_retry_attempted = true;
                    upstream_native_pending = false;
                    pd_verifier.set_pd_verified(true)?;
                    AutoPhase::EnablingPublic(Instant::now() + PUBLIC_REASSERT_DELAY)
                } else {
                    AutoPhase::Settled
                }
            }
            other => other,
        };
    }

    unsafe {
        libc::close(uevent_sock);
    }
    if let Err(error) = broadcast_stop_event.notify() {
        error!("通知broadcast-forger线程停止失败: {}", error);
    }
    if let Err(error) = broadcast_handle.join() {
        error!("broadcast-forger线程join失败: {:?}", error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AutoPhase, can_start_public_retry, public_pps_activation_succeeded,
        should_capture_upstream_detach, should_enable_public_early, should_process_physical_detach,
        should_rearm_public_retry, should_start_native_wait_after_upstream_attach,
    };
    use std::time::Instant;

    #[test]
    fn physical_detach_is_processed_without_software_reconnect_masking() {
        assert!(should_process_physical_detach(true, false));
        assert!(!should_process_physical_detach(true, true));
        assert!(!should_process_physical_detach(false, false));
    }

    #[test]
    fn public_activation_requires_verified_pps_state() {
        assert!(public_pps_activation_succeeded("1", "PD_PPS"));
        assert!(!public_pps_activation_succeeded("1", "PD"));
        assert!(!public_pps_activation_succeeded("0", "PD_PPS"));
    }

    #[test]
    fn early_public_activation_requires_unverified_public_svid() {
        assert!(should_enable_public_early("0", "PD_PPS", "0000"));
        assert!(!should_enable_public_early("1", "PD_PPS", "0000"));
        assert!(!should_enable_public_early("0", "PD", "0000"));
        assert!(!should_enable_public_early("0", "PD_PPS", "2717"));
        assert!(!should_enable_public_early("0", "PD_PPS", ""));
    }

    #[test]
    fn public_retry_budget_is_bounded() {
        assert!(can_start_public_retry(0));
        assert!(can_start_public_retry(1));
        assert!(!can_start_public_retry(2));
    }

    #[test]
    fn upstream_swap_reenters_native_wait_only_after_charging_returns() {
        assert!(should_start_native_wait_after_upstream_attach(
            true, "Charging", "1"
        ));
        assert!(!should_start_native_wait_after_upstream_attach(
            true,
            "Discharging",
            "1"
        ));
        assert!(!should_start_native_wait_after_upstream_attach(
            true, "Charging", "0"
        ));
        assert!(!should_start_native_wait_after_upstream_attach(
            false, "Charging", "1"
        ));
    }

    #[test]
    fn stable_native_or_public_sessions_can_capture_upstream_detach() {
        assert!(should_capture_upstream_detach(AutoPhase::Settled, true));
        assert!(!should_capture_upstream_detach(AutoPhase::Settled, false));
        assert!(!should_capture_upstream_detach(
            AutoPhase::EnablingPublic(Instant::now()),
            true
        ));
        assert!(!should_capture_upstream_detach(
            AutoPhase::VerifyingPublic(Instant::now()),
            true
        ));
    }

    #[test]
    fn upstream_retry_rearm_requires_stable_offline_evidence() {
        assert!(!should_rearm_public_retry(
            false,
            true,
            "0",
            "Unknown",
            "Discharging",
            "0"
        ));
        assert!(!should_rearm_public_retry(
            true,
            false,
            "0",
            "Unknown",
            "Discharging",
            "0"
        ));
        assert!(!should_rearm_public_retry(
            true, true, "0", "Unknown", "Charging", "0"
        ));
        assert!(!should_rearm_public_retry(
            true,
            true,
            "0",
            "Unknown",
            "Discharging",
            "1"
        ));
        assert!(should_rearm_public_retry(
            true,
            true,
            "0",
            "Unknown",
            "Discharging",
            "0"
        ));
    }
}

#[cfg(unix)]
fn start_charging_session(
    charging_session_active: &mut bool,
    session_gen: &AtomicU32,
    session_active: &AtomicBool,
    session_event: &EventFd,
) {
    if !*charging_session_active {
        *charging_session_active = true;
        session_active.store(true, Ordering::Relaxed);
        session_gen.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = session_event.notify() {
            warn!("通知broadcast-forger会话开始失败: {}", error);
        }
    }
}

#[cfg(unix)]
fn stop_charging_session(
    charging_session_active: &mut bool,
    session_active: &AtomicBool,
    session_event: &EventFd,
) {
    *charging_session_active = false;
    session_active.store(false, Ordering::Relaxed);
    if let Err(error) = session_event.notify() {
        warn!("通知broadcast-forger会话结束失败: {}", error);
    }
}
