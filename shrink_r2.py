"""Re-encode every chapter MP3 in the R2 bucket as 48 kbps mono at 24 kHz, in place.

Same key, so the site needs no change. Each file is checked before it replaces
the old one (decodes, same length to within half a second, actually smaller);
anything that fails is left exactly as it was and listed at the end. A
converted file carries `format: b5` in its metadata, so a rerun skips it
without downloading: stop at any time and run again.

Runs in GitHub Actions (see .github/workflows/shrink.yml), split across
--parts machines by a hash of the key.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config

AUDIO_RE = re.compile(r'^data/([^/]+)/audio/\d+\.mp3$')
MARK = 'b5'
MAX_DRIFT_S = 0.5


def client():
    return boto3.client(
        's3',
        endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
        region_name='auto',
        config=Config(retries={'max_attempts': 10, 'mode': 'adaptive'}, max_pool_connections=32),
    )


def probe(path):
    out = subprocess.run(
        ['ffprobe', '-v', 'error', '-select_streams', 'a:0',
         '-show_entries', 'stream=sample_rate,bit_rate:format=duration', '-of', 'json', path],
        capture_output=True, text=True).stdout
    d = json.loads(out or '{}')
    st = (d.get('streams') or [{}])[0]
    return (int(st.get('sample_rate') or 0), int(st.get('bit_rate') or 0),
            float((d.get('format') or {}).get('duration') or 0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--part', type=int, default=0)
    ap.add_argument('--parts', type=int, default=1)
    ap.add_argument('--dry-run', action='store_true', help='convert and check, but write nothing to R2')
    ap.add_argument('--threads', type=int, default=os.cpu_count() or 2)
    args = ap.parse_args()
    slug = (os.environ.get('SLUG') or '').strip().strip('/')
    if slug and not re.fullmatch(r'[a-z0-9-]+', slug):
        sys.exit(f'bad slug: {slug!r}')
    bucket = os.environ.get('R2_BUCKET', 'novel')
    s3 = client()

    keys = []
    prefix = f'data/{slug}/audio/' if slug else 'data/'
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get('Contents', []):
            k = o['Key']
            if AUDIO_RE.match(k) and zlib.crc32(k.encode()) % args.parts == args.part:
                keys.append((k, o['Size']))
    print(f'part {args.part}/{args.parts}: {len(keys)} audio files', flush=True)

    lock = threading.Lock()
    stats = {'ok': 0, 'skip': 0, 'already': 0, 'fail': 0, 'before': 0, 'after': 0}
    fails = []
    done = [0]

    def one(item):
        key, size = item
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            if (head.get('Metadata') or {}).get('format') == MARK:
                return 'skip', key, size, size, ''
            with tempfile.TemporaryDirectory() as d:
                src, dst = os.path.join(d, 'in.mp3'), os.path.join(d, 'out.mp3')
                s3.download_file(bucket, key, src)
                sr, br, dur = probe(src)
                if dur <= 0:
                    return 'fail', key, size, size, 'cannot read original'
                extra = {'ContentType': head.get('ContentType') or 'audio/mpeg', 'Metadata': {'format': MARK}}
                if head.get('CacheControl'):
                    extra['CacheControl'] = head['CacheControl']
                if sr and sr <= 24000 and br and br <= 56000:
                    # Already small (made by the new pipeline): just mark it.
                    if not args.dry_run:
                        s3.copy_object(Bucket=bucket, Key=key, CopySource={'Bucket': bucket, 'Key': key},
                                       MetadataDirective='REPLACE', **extra)
                    return 'already', key, size, size, ''
                r = subprocess.run(
                    ['ffmpeg', '-v', 'error', '-y', '-i', src, '-map_metadata', '-1', '-ac', '1', '-ar', '24000',
                     '-codec:a', 'libmp3lame', '-b:a', '48k', dst], capture_output=True, text=True)
                if r.returncode or not os.path.exists(dst):
                    return 'fail', key, size, size, 'ffmpeg: ' + r.stderr[-200:]
                _, _, new_dur = probe(dst)
                new_size = os.path.getsize(dst)
                if abs(new_dur - dur) > MAX_DRIFT_S:
                    return 'fail', key, size, size, f'length {dur:.1f}s -> {new_dur:.1f}s'
                if new_size >= size or new_size < 10_000:
                    return 'fail', key, size, size, f'size {size} -> {new_size}'
                if not args.dry_run:
                    s3.upload_file(dst, bucket, key, ExtraArgs=extra)
                return 'ok', key, size, new_size, ''
        except Exception as e:  # network, permissions...
            return 'fail', key, size, size, f'{type(e).__name__}: {e}'[:200]

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        for status, key, before, after, why in pool.map(one, keys):
            with lock:
                stats[status] += 1
                stats['before'] += before
                stats['after'] += after
                done[0] += 1
                if why:
                    fails.append((key, why))
                if done[0] % 100 == 0 or done[0] == len(keys):
                    print(f'  {done[0]}/{len(keys)}  ok={stats["ok"]} skip={stats["skip"]} '
                          f'already={stats["already"]} fail={stats["fail"]}  '
                          f'{stats["before"]/1024**3:.2f} GB -> {stats["after"]/1024**3:.2f} GB  '
                          f'({time.time()-t0:.0f}s)', flush=True)

    summary = (f'### Part {args.part}/{args.parts}{" (dry run)" if args.dry_run else ""}\n\n'
               f'| converted | skipped (done before) | already small | failed | before | after |\n'
               f'|---|---|---|---|---|---|\n'
               f'| {stats["ok"]} | {stats["skip"]} | {stats["already"]} | {stats["fail"]} | '
               f'{stats["before"]/1024**3:.2f} GB | {stats["after"]/1024**3:.2f} GB |\n')
    if fails:
        summary += '\nFailed (left untouched):\n\n' + '\n'.join(f'- `{k}`: {w}' for k, w in fails[:50]) + '\n'
    print(summary)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a', encoding='utf-8') as f:
            f.write(summary)


if __name__ == '__main__':
    main()
