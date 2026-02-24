# Databricks notebook source
from typing import Dict
from pyspark.sql import SparkSession

def mount_adls_container(
    spark: SparkSession,
    storage_account: str,
    container: str,
    account_key: str,
    mount_point: str
) -> None:
    """
    Mounts an ADLS Gen2 container to DBFS.
    """
    configs: Dict[str, str] = {
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net": account_key
    }

    # Check if already mounted
    mounts = [m.mountPoint for m in dbutils.fs.mounts()]
    if mount_point not in mounts:
        dbutils.fs.mount(
            source=f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs
        )
