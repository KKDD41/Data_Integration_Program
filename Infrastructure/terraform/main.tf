terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~>3.0"
    }
    databricks = {
      source = "databricks/databricks"
    }
  }
}

provider "azurerm" {

  features {

    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

variable "initials" {
  default = "ed"
}

variable "location" {
  default = "West Europe"
}

variable "sql_admin_password" {
  description = "admin"
  type        = string
}

resource "azurerm_resource_group" "rg_di_mentoring" {
  name     = "rg-di-mentoring-${var.initials}"
  location = var.location
}

resource "azurerm_storage_account" "stdi_mentoring_blob" {
  name                     = "stdiblobacc${var.initials}"
  resource_group_name      = azurerm_resource_group.rg_di_mentoring.name
  location                 = azurerm_resource_group.rg_di_mentoring.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  is_hns_enabled = true
}

resource "azurerm_storage_data_lake_gen2_filesystem" "stdi_mentoring_data_lake" {
  name                     = "stdidatalake${var.initials}"
  storage_account_id       = azurerm_storage_account.stdi_mentoring_blob.id
}

resource "azurerm_storage_container" "data_container" {
  name                  = "data"
  storage_account_name  = azurerm_storage_data_lake_gen2_filesystem.stdi_mentoring_data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_data_lake_gen2_path" "bronze_folder" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.stdi_mentoring_data_lake.name
  storage_account_id = azurerm_storage_account.stdi_mentoring_blob.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver_folder" {
  path               = "silver"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.stdi_mentoring_data_lake.name
  storage_account_id = azurerm_storage_account.stdi_mentoring_blob.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold_folder" {
  path               = "gold"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.stdi_mentoring_data_lake.name
  storage_account_id = azurerm_storage_account.stdi_mentoring_blob.id
  resource           = "directory"
}

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv_di_mentoring" {
  name                = "kv-di-mentoring-${var.initials}"
  location            = azurerm_resource_group.rg_di_mentoring.location
  resource_group_name = azurerm_resource_group.rg_di_mentoring.name
  sku_name            = "standard"
  tenant_id           = data.azurerm_client_config.current.tenant_id

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get",
      "List",
      "Create",
      "Delete",
    ]

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete",
    ]
  }
}

resource "azurerm_synapse_workspace" "syn_di_mentoring" {
  name                                 = "syn-di-mentoring-${var.initials}"
  resource_group_name                  = azurerm_resource_group.rg_di_mentoring.name
  location                             = azurerm_resource_group.rg_di_mentoring.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.stdi_mentoring_data_lake.id

  sql_administrator_login          = "sqladminuser"
  sql_administrator_login_password = var.sql_admin_password

  managed_resource_group_name = "rg-di-mng-synapse-${var.initials}"

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_databricks_workspace" "dbs_di_mentoring" {
  name                = "dbs-di-mentoring-${var.initials}"
  resource_group_name = azurerm_resource_group.rg_di_mentoring.name
  location            = azurerm_resource_group.rg_di_mentoring.location
  sku                 = "premium"
}