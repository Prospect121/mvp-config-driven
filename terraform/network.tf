# Configuración de red para endpoints privados y conectividad segura

# Virtual Network
resource "azurerm_virtual_network" "main" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${var.project_name}-${var.environment}-vnet"
  address_space       = var.vnet_address_space
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  tags = local.common_tags
}

# Subnet para servicios de datos
resource "azurerm_subnet" "data_services" {
  count                = var.enable_private_endpoints ? 1 : 0
  name                 = "data-services-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = [var.subnet_address_prefixes.data_services]

  # Service endpoints para servicios de Azure
  service_endpoints = [
    "Microsoft.Storage",
    "Microsoft.Sql",
    "Microsoft.EventHub",
    "Microsoft.KeyVault"
  ]

  # Nota: Azure Data Factory no requiere delegación de subnet específica
}

# Subnet para endpoints privados
resource "azurerm_subnet" "private_endpoints" {
  count                = var.enable_private_endpoints ? 1 : 0
  name                 = "private-endpoints-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = [var.subnet_address_prefixes.private_endpoints]

  # Deshabilitar políticas de red para endpoints privados
  private_endpoint_network_policies = "Disabled"
}

# Network Security Group para servicios de datos
resource "azurerm_network_security_group" "data_services" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${var.project_name}-${var.environment}-data-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  # Regla para permitir tráfico HTTPS
  security_rule {
    name                       = "AllowHTTPS"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  # Regla para permitir tráfico SQL
  security_rule {
    name                       = "AllowSQL"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "1433"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  # Regla para permitir Event Hub
  security_rule {
    name                       = "AllowEventHub"
    priority                   = 1003
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["5671", "5672", "443"]
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  # Denegar todo el resto del tráfico
  security_rule {
    name                       = "DenyAll"
    priority                   = 4096
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = local.common_tags
}

# Asociación del NSG con la subnet de servicios de datos
resource "azurerm_subnet_network_security_group_association" "data_services" {
  count                     = var.enable_private_endpoints ? 1 : 0
  subnet_id                 = azurerm_subnet.data_services[0].id
  network_security_group_id = azurerm_network_security_group.data_services[0].id
}

# Route Table para controlar el enrutamiento
resource "azurerm_route_table" "main" {
  count                         = var.enable_private_endpoints ? 1 : 0
  name                          = "${var.project_name}-${var.environment}-rt"
  location                      = azurerm_resource_group.main.location
  resource_group_name           = azurerm_resource_group.main.name
  bgp_route_propagation_enabled = true

  # Ruta para forzar tráfico a través de Azure Firewall (opcional)
  # route {
  #   name           = "DefaultRoute"
  #   address_prefix = "0.0.0.0/0"
  #   next_hop_type  = "VirtualAppliance"
  #   next_hop_in_ip_address = "10.0.100.4"  # IP del Azure Firewall
  # }

  tags = local.common_tags
}

# Asociación de la Route Table con las subnets
resource "azurerm_subnet_route_table_association" "data_services" {
  count          = var.enable_private_endpoints ? 1 : 0
  subnet_id      = azurerm_subnet.data_services[0].id
  route_table_id = azurerm_route_table.main[0].id
}

resource "azurerm_subnet_route_table_association" "private_endpoints" {
  count          = var.enable_private_endpoints ? 1 : 0
  subnet_id      = azurerm_subnet.private_endpoints[0].id
  route_table_id = azurerm_route_table.main[0].id
}

# VPN Gateway para conectividad on-premises (opcional)
resource "azurerm_subnet" "gateway" {
  count                = var.enable_private_endpoints ? 1 : 0
  name                 = "GatewaySubnet"  # Nombre requerido por Azure
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = ["10.0.255.0/27"]  # Subnet dedicada para gateway
}

resource "azurerm_public_ip" "vpn_gateway" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${var.project_name}-${var.environment}-vpn-pip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = local.common_tags
}

# Comentado por defecto - descomentar si se necesita VPN Gateway
# resource "azurerm_virtual_network_gateway" "vpn" {
#   count               = var.enable_private_endpoints ? 1 : 0
#   name                = "${var.project_name}-${var.environment}-vpn-gw"
#   location            = azurerm_resource_group.main.location
#   resource_group_name = azurerm_resource_group.main.name
#   
#   type     = "Vpn"
#   vpn_type = "RouteBased"
#   
#   active_active = false
#   enable_bgp    = false
#   sku           = "VpnGw1"
#   
#   ip_configuration {
#     name                          = "vnetGatewayConfig"
#     public_ip_address_id          = azurerm_public_ip.vpn_gateway[0].id
#     private_ip_address_allocation = "Dynamic"
#     subnet_id                     = azurerm_subnet.gateway[0].id
#   }
#   
#   tags = local.common_tags
# }

# Bastion Host para acceso seguro (opcional)
resource "azurerm_subnet" "bastion" {
  count                = var.enable_private_endpoints ? 1 : 0
  name                 = "AzureBastionSubnet"  # Nombre requerido por Azure
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main[0].name
  address_prefixes     = ["10.0.254.0/27"]  # Subnet dedicada para Bastion
}

resource "azurerm_public_ip" "bastion" {
  count               = var.enable_private_endpoints ? 1 : 0
  name                = "${var.project_name}-${var.environment}-bastion-pip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = local.common_tags
}

# Comentado por defecto - descomentar si se necesita Bastion Host
# resource "azurerm_bastion_host" "main" {
#   count               = var.enable_private_endpoints ? 1 : 0
#   name                = "${var.project_name}-${var.environment}-bastion"
#   location            = azurerm_resource_group.main.location
#   resource_group_name = azurerm_resource_group.main.name
#   
#   ip_configuration {
#     name                 = "configuration"
#     subnet_id            = azurerm_subnet.bastion[0].id
#     public_ip_address_id = azurerm_public_ip.bastion[0].id
#   }
#   
#   tags = local.common_tags
# }