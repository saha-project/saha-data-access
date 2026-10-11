# Resume the Xenium mirror to Cayuga. Safe to re-run; --partial continues.
#
# Retries because a single rsync does not survive the VPN: the 2026-09-22 run
# died at "Can't assign requested address" two minutes after the tunnel dropped
# and left 3.3 of 68 GB transferred, with nothing to restart it.
cd "/Users/jiwoonpark/Dropbox (Personal)/2025- MasonLab/2025_SAHA/Data/preprocessed/spatialdata/RNA"

for attempt in $(seq 1 200); do
  rsync -a --partial --exclude=.DS_Store --exclude="._*" \
    -e "ssh -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=6" \
    ./ cayuga-login1.cac.cornell.edu:/athena/project-saha/derived/xenium_zarr/
  rc=$?
  [ $rc -eq 0 ] && { echo "[$(date)] mirror complete after $attempt attempt(s)"; exit 0; }
  echo "[$(date)] attempt $attempt exited $rc; retrying in 60s"
  sleep 60
done
echo "[$(date)] gave up after 200 attempts"
exit 1
