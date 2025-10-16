# mkts-north Scheduled Execution

This project uses systemd timers (instead of cron) to run `mkts-north` automatically every 2 hours.

## Timer Files

- `~/.config/systemd/user/mkts-north.service` - The service that runs the command
- `~/.config/systemd/user/mkts-north.timer` - The timer that triggers every 2 hours

## Schedule

- **First run:** 5 minutes after boot
- **Subsequent runs:** Every 2 hours after the previous run completes

## Management Commands

### Check timer status
```bash
systemctl --user status mkts-north.timer
```

### See when it will run next
```bash
systemctl --user list-timers mkts-north.timer
```

### Manually trigger a run now
```bash
systemctl --user start mkts-north.service
```

### View logs from recent runs
```bash
journalctl --user -u mkts-north.service
```

### View logs with follow mode (live updates)
```bash
journalctl --user -u mkts-north.service -f
```

### Stop the timer
```bash
systemctl --user stop mkts-north.timer
```

### Disable the timer (won't start on boot)
```bash
systemctl --user disable mkts-north.timer
```

### Re-enable the timer
```bash
systemctl --user enable mkts-north.timer
systemctl --user start mkts-north.timer
```

### Reload systemd configuration (after editing service/timer files)
```bash
systemctl --user daemon-reload
```

## Troubleshooting

If the timer isn't running:

1. Check if the timer is enabled and active:
   ```bash
   systemctl --user status mkts-north.timer
   ```

2. Check recent service logs for errors:
   ```bash
   journalctl --user -u mkts-north.service -n 50
   ```

3. Test the service manually:
   ```bash
   systemctl --user start mkts-north.service
   journalctl --user -u mkts-north.service -f
   ```

4. Verify the timer is scheduled:
   ```bash
   systemctl --user list-timers --all
   ```
