// Package sdnotify implements the systemd notify protocol (sd_notify) without
// cgo or libsystemd: READY=1 at start-up and WATCHDOG=1 keep-alives.
package sdnotify

import (
	"net"
	"os"
	"strconv"
	"time"
)

// Notify sends a state string to systemd. It is a no-op when not running
// under systemd with Type=notify.
func Notify(state string) error {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return nil
	}
	if sock[0] == '@' {
		sock = "\x00" + sock[1:] // abstract socket
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: sock, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.Write([]byte(state))
	return err
}

// WatchdogInterval returns how often to send WATCHDOG=1 (half of
// WatchdogSec), or 0 if the watchdog is not enabled.
func WatchdogInterval() time.Duration {
	us, err := strconv.ParseInt(os.Getenv("WATCHDOG_USEC"), 10, 64)
	if err != nil || us <= 0 {
		return 0
	}
	if pid := os.Getenv("WATCHDOG_PID"); pid != "" && pid != strconv.Itoa(os.Getpid()) {
		return 0
	}
	return time.Duration(us) * time.Microsecond / 2
}
