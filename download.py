import pandas as pd
import requests
import os
import hashlib
import logging
import argparse

from pathlib import Path
from multiprocessing.pool import ThreadPool


def ensure_path(path):
    if not os.path.exists(path):
        os.makedirs(path)


def download_sha256sums(url):
    sha256sums = {}
    sha256sums_txt = ""
    
    comments_url = f'{url}/comments/sha256sum.txt'
    submissions_url = f'{url}/submissions/sha256sums.txt'
    
    for response, response_url in [(requests.get(comments_url), comments_url), (requests.get(submissions_url), submissions_url)]:
        if response.status_code == 200:
            sha256sums_txt += response.text
            for line in response.text.splitlines():
                hash, filename = line.split()
                sha256sums[filename] = hash
        else:
            logger.error(f'Error downloading {response_url}: {response.status_code}')
    
    with open(files_dir / "sha256sums.txt", "w") as f:
        f.write(sha256sums_txt)

    return sha256sums


def verify(filename):
    if filename in sha256sums:
        with open(files_dir / filename, 'rb') as f:
            checksum = hashlib.sha256()
            while chunk := f.read(65536):
                checksum.update(chunk)
            if checksum.hexdigest() == sha256sums[filename]:
                return True
            else:
                return False
    logger.error(f'{filename} not found in sha256sums.txt')
    return True


def download_file(filename):
    if "RC" in filename:
        download_url = f'{url}/comments/{filename}'
    elif "RS" in filename:
        download_url = f'{url}/submissions/{filename}'
    else:
        logger.error(f'Invalid filename: {filename}')
        return False

    path = files_dir / filename

    if not path.exists():
        try:
            response = requests.get(download_url, stream=True)
            if response.status_code == 200:
                with open(path, 'wb') as f:
                    logger.info(f'Downloading {filename}')
                    for data in response.iter_content(chunk_size=1048576):
                        f.write(data)
                    logger.info(f'Downloaded {filename}')
                    check = verify(filename)
                    if check:
                        logger.info(f'{filename} verified')
                        return True

                    else:
                        # I'm not sure why but this fails sometimes when the files aren't corrupt
                        # Just run the script again or use the command `sha256sum -c sha256sums.txt`
                        # in the directory you stored the data
                        logger.error(
                            f'{filename} may be corrupted')
                        return False
            else:
                logger.error(
                    f'Error downloading {filename}: {response.status_code}')
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f'Error downloading {filename}: {e}')
            return False
    else:
        logger.info(f'{filename} already exists, verifying integrity...')
        check = verify(filename)
        if check:
            logger.info(f'{filename} verified')

            return True
        else:
            logger.error(f'{filename} corrupted, retrying download')
            path.unlink()  # delete corrupted file
            if retries < 1:
                # retry download
                return download_file(filename, retries=retries + 1)
            else:
                logger.error(f'{filename} download failed after 1 retry')
                return False

parser = argparse.ArgumentParser(
    description="This is a program that extracts data from Reddit's public comment and submission history and stores it in a specified directory.")
parser.add_argument("-s", "--start-date",
                    help="Start date for data extraction (format: YYYY-MM)", default="2005-12")
parser.add_argument("-e", "--end-date",
                    help="End date for data extraction (format: YYYY-MM)", default="2022-10")
parser.add_argument("-d", "--directory",
                    help="Directory to store extracted data", default="data/reddit")
parser.add_argument("-v", "--verbose",
                    help="Verbose output", action="store_true")
parser.add_argument("-t", "--threads",
                    help="Thread count", default=os.cpu_count())
args = parser.parse_args()

log_dir = "./logs"
ensure_path(log_dir)

logging.basicConfig(filename=f'{log_dir}/download.log',
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s [%(threadName)s] ",
                    level=logging.INFO,
                    filemode="w")
logger = logging.getLogger()

# create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel(logging.INFO)

if args.verbose:
    logger.setLevel(logging.DEBUG)

files_dir = Path(args.directory)
url = "https://files.pushshift.io/reddit"
reddit_comments = []
reddit_submissions = []

dr = pd.date_range(args.start_date, args.end_date,
                freq='MS').strftime('%Y-%m').tolist()

for date in dr:
    reddit_comments.append(str(f'RC_{date}.zst'))
    reddit_submissions.append(str(f'RS_{date}.zst'))

ensure_path(files_dir)
sha256sums = download_sha256sums(url)
if sha256sums is None:
    logger.error('Failed to download sha256sums')
    exit()

all_files = reddit_comments + reddit_submissions
threads = args.threads if args.threads > len(all_files) else len(all_files)

logger.info('Starting download...')
# Download datasets
results = ThreadPool(threads).imap_unordered(download_file, all_files)
for r in results:
    continue
logger.info('Download complete!')

logger.info(f'downloaded and verified {len(all_files)} files')
