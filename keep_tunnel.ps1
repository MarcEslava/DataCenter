# SSH tunnel: Windows host → cecotest3 → BI SQL Server (185.254.207.228:1433)
# Binds 0.0.0.0:1433 so Docker containers reach it via host.docker.internal:1433
#
# Run with:  .\keep_tunnel.ps1
# Or as a background job:  Start-Job -FilePath .\keep_tunnel.ps1

$SSH_HOST     = "cecotest3.ecoceutics.com"
$SSH_PORT     = 12984
$SSH_USER     = "kkoEMhXWsS4gFb"
$SSH_KEY      = "$env:USERPROFILE\.ssh\id_ed25519"
$LOCAL_PORT   = 1433
$REMOTE_HOST  = "185.254.207.228"
$REMOTE_PORT  = 1433

Write-Host "Starting SSH tunnel on 0.0.0.0:$LOCAL_PORT → $REMOTE_HOST`:$REMOTE_PORT via $SSH_HOST`:$SSH_PORT"

while ($true) {
    ssh -N `
        -p $SSH_PORT `
        -i $SSH_KEY `
        -L "0.0.0.0:${LOCAL_PORT}:${REMOTE_HOST}:${REMOTE_PORT}" `
        -o ServerAliveInterval=30 `
        -o ServerAliveCountMax=3 `
        -o StrictHostKeyChecking=no `
        -o ExitOnForwardFailure=yes `
        "${SSH_USER}@${SSH_HOST}"

    Write-Host "$(Get-Date -Format 'HH:mm:ss') Tunnel dropped, reconnecting in 5s..."
    Start-Sleep 5
}
