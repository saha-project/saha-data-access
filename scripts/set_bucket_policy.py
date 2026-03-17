#!/usr/bin/env python3
"""
set_bucket_policy.py
Configure S3 bucket policies for the SAHA open data release.

Creates two buckets:
  saha-open-data       — public read on processed/ zarr/ metadata/ docs/
                         raw/cosmx/ remains private (controlled via policy)
  saha-registered-data — private; access via IAM/pre-signed URLs for
                         registered-tier samples (future use)

Usage:
    # Show policies without applying
    python scripts/set_bucket_policy.py --dry-run

    # Apply open-data bucket policy
    python scripts/set_bucket_policy.py --bucket saha-open-data

    # Create registered-data bucket
    python scripts/set_bucket_policy.py --bucket saha-registered-data --registered

Requirements:
    pip install boto3
    AWS credentials with s3:PutBucketPolicy, s3:CreateBucket permissions.
"""

import argparse
import json
import sys

OPEN_BUCKET       = "saha-open-data"
REGISTERED_BUCKET = "saha-registered-data"
REGION            = "us-east-1"

# ---------------------------------------------------------------------------
# Bucket policies
# ---------------------------------------------------------------------------

def open_data_policy(bucket: str) -> dict:
    """
    Allow anonymous s3:GetObject on processed/, zarr/, metadata/, docs/.
    Raw CosMx flat files under raw/ are NOT public (require AWS credentials).
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadProcessed",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": [
                    f"arn:aws:s3:::{bucket}/processed/*",
                    f"arn:aws:s3:::{bucket}/zarr/*",
                    f"arn:aws:s3:::{bucket}/metadata/*",
                    f"arn:aws:s3:::{bucket}/docs/*",
                    f"arn:aws:s3:::{bucket}/README.md",
                ],
            },
            {
                "Sid": "PublicListMetadata",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:ListBucket",
                "Resource": f"arn:aws:s3:::{bucket}",
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            "processed/*",
                            "zarr/*",
                            "metadata/*",
                            "docs/*",
                        ]
                    }
                },
            },
        ],
    }


def registered_bucket_policy(bucket: str) -> dict:
    """
    No public access. Access granted per IAM user/role via separate policy.
    This is a placeholder — attach individual IAM policies to registered users.
    """
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DenyPublicAccess",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{bucket}",
                    f"arn:aws:s3:::{bucket}/*",
                ],
                "Condition": {
                    "StringNotEquals": {
                        "aws:PrincipalType": ["IAMUser", "AssumedRole", "FederatedUser"]
                    }
                },
            }
        ],
    }


# ---------------------------------------------------------------------------
# CORS configuration (allow browser-based access for portals/notebooks)
# ---------------------------------------------------------------------------

CORS_CONFIG = {
    "CORSRules": [
        {
            "AllowedHeaders": ["*"],
            "AllowedMethods": ["GET", "HEAD"],
            "AllowedOrigins": ["*"],
            "ExposeHeaders":  ["ETag", "Content-Length"],
            "MaxAgeSeconds":  3600,
        }
    ]
}


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def ensure_bucket(s3, bucket: str, region: str, dry_run: bool) -> None:
    """Create bucket if it does not exist."""
    if dry_run:
        print(f"  [dry-run] ensure bucket s3://{bucket} in {region}")
        return
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"  Bucket s3://{bucket} already exists.")
    except s3.exceptions.ClientError:
        kwargs = {"Bucket": bucket}
        if region != "us-east-1":
            kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
        s3.create_bucket(**kwargs)
        # Block all public access by default (then selectively open)
        s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )
        print(f"  Created bucket s3://{bucket}")


def apply_policy(s3, bucket: str, policy: dict, dry_run: bool) -> None:
    policy_str = json.dumps(policy, indent=2)
    if dry_run:
        print(f"\n  [dry-run] Policy for s3://{bucket}:")
        print(policy_str)
        return
    s3.put_bucket_policy(Bucket=bucket, Policy=policy_str)
    print(f"  Applied bucket policy to s3://{bucket}")


def apply_cors(s3, bucket: str, dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] CORS config for s3://{bucket}")
        return
    s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=CORS_CONFIG)
    print(f"  Applied CORS config to s3://{bucket}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket",     default=OPEN_BUCKET)
    p.add_argument("--region",     default=REGION)
    p.add_argument("--registered", action="store_true",
                   help="Configure the registered-data bucket instead")
    p.add_argument("--dry-run",    action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    try:
        import boto3
        s3 = boto3.client("s3", region_name=args.region)
    except ImportError:
        sys.exit("ERROR: boto3 not installed. pip install boto3")

    if args.registered:
        bucket = REGISTERED_BUCKET
        policy = registered_bucket_policy(bucket)
        print(f"Configuring registered-data bucket: s3://{bucket}")
    else:
        bucket = args.bucket
        policy = open_data_policy(bucket)
        print(f"Configuring open-data bucket: s3://{bucket}")

    ensure_bucket(s3, bucket, args.region, args.dry_run)
    apply_policy(s3, bucket, policy, args.dry_run)
    apply_cors(s3, bucket, args.dry_run)

    if not args.dry_run:
        print("\nVerify anonymous access:")
        print(f"  aws s3 ls s3://{bucket}/metadata/ --no-sign-request")


if __name__ == "__main__":
    main()
