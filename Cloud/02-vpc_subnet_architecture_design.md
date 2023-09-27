# Assignment-2: VPC Subnet Architecture Design
NutBodyslam053@TCC231019
vpc subnet architecture design

## Create subnets (vSwitches) within a VPC with the CIDR range 10.0.0.0/8
> The CIDR range `10.0.0.0/8` encompasses `16,777,216` IP addresses.

### 1. Optimizing IP address allocation
- **Public Subnets (4 zones)**:
  - Divide the public IP space further by using the CIDR range `10.0.0.0/10` to create 4 subnets, one for each availability zone.
  - The CIDR range for each subnet extends from `10.0.0.0` to `10.127.255.255`, providing a total of `8,388,608` IP addresses, thereby allowing `2,097,152` IP addresses within each public subnet.
    - 10.0.0.0 - 10.31.255.255     ⇒ (2,097,152)
    - 10.32.0.0 - 10.63.255.255    ⇒ (2,097,152)
    - 10.64.0.0 - 10.95.255.255    ⇒ (2,097,152)
    - 10.96.0.0 - 10.127.255.255   ⇒ (2,097,152)

- **Private Subnets (4 zones)**:
   - Divide the private IP space further by using the CIDR range `10.0.0.0/10` to create 4 subnets, one for each availability zone.
   - Each private subnet can have a CIDR range from `10.128.0.0` to `10.255.255.255`, providing a total of `8,388,608` IP addresses, thereby allowing `2,097,152` IP addresses within each private subnet.
     - 10.128.0.0 - 10.159.255.255 ⇒ (2,097,152)
     - 10.160.0.0 - 10.191.255.255 ⇒ (2,097,152)
     - 10.192.0.0 - 10.223.255.255 ⇒ (2,097,152)
     - 10.224.0.0 - 10.255.255.255 ⇒ (2,097,152)

- **Remaining IP Addresses**:
   - There are no remaining IP addresses available!

```bash
 ┌─ <VPC> 10.0.0.0/8 (16,777,216) ────────────────────────────────────────────────────┐
 │                                                                                    │
 │  ┌─ <AZ-01> 10.0.0.0/10 (4,194,304) ───┐  ┌─ <AZ-02> 10.0.0.0/10 (4,194,304) ───┐  │       
 │  │                                     │  │                                     │  │          
 │  │ ┌─ <Public Subnet> ───────────────┐ │  │ ┌─ <Public Subnet> ───────────────┐ │  │               
 │  │ │     10.0.0.0 - 10.31.255.255    │ │  │ │     10.32.0.0 - 10.63.255.255   │ │  │               
 │  │ │           (2,097,152)           │ │  │ │           (2,097,152)           │ │  │       
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │     
 │  │ ┌─ <Private Subnet> ──────────────┐ │  │ ┌─ <Private Subnet> ──────────────┐ │  │        
 │  │ │   10.128.0.0 - 10.159.255.255   │ │  │ │   10.160.0.0 - 10.191.255.255   │ │  │        
 │  │ │           (2,097,152)           │ │  │ │           (2,097,152)           │ │  │        
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │      
 │  └─────────────────────────────────────┘  └─────────────────────────────────────┘  │    
 │  ┌─ <AZ-03> 10.0.0.0/10 (4,194,304) ───┐  ┌─ <AZ-04> 10.0.0.0/10 (4,194,304) ───┐  │                      
 │  │                                     │  │                                     │  │                         
 │  │ ┌─ <Public Subnet> ───────────────┐ │  │ ┌─ <Public Subnet> ───────────────┐ │  │                              
 │  │ │    10.64.0.0 - 10.95.255.255    │ │  │ │    10.96.0.0 - 10.127.255.255   │ │  │                              
 │  │ │           (2,097,152)           │ │  │ │           (2,097,152)           │ │  │                      
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │   
 │  │ ┌─ <Private Subnet> ──────────────┐ │  │ ┌─ <Private Subnet> ──────────────┐ │  │                       
 │  │ │   10.192.0.0 - 10.223.255.255   │ │  │ │   10.224.0.0 - 10.255.255.255   │ │  │      
 │  │ │           (2,097,152)           │ │  │ │           (2,097,152)           │ │  │       
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │         
 │  └─────────────────────────────────────┘  └─────────────────────────────────────┘  │           
 │                                                                                    │           
 └────────────────────────────────────────────────────────────────────────────────────┘  
```

### 2. Remaining IP space for other purposes
- **Public Subnets (4 zones)**:
  - Divide the public IP space further by using the CIDR range `10.0.0.0/29` to create 4 subnets, one for each availability zone.
  - The CIDR range for each subnet extends from `10.0.0.0` to `10.0.0.15`, providing a total of `16` IP addresses, thereby allowing `4` IP addresses within each public subnet.
    - 10.0.0.0 - 10.0.0.3     ⇒ (4)
    - 10.0.0.4 - 10.0.0.7    ⇒ (4)
    - 10.0.0.8 - 10.0.0.11   ⇒ (4)
    - 10.0.0.12 - 10.0.0.15   ⇒ (4)

- **Private Subnets (4 zones)**:
   - Divide the private IP space further by using the CIDR range `10.0.0.0/29` to create 4 subnets, one for each availability zone.
   - Each private subnet can have a CIDR range from `10.0.0.16` to `10.0.0.31`, providing a total of `16` IP addresses, thereby allowing `4` IP addresses within each private subnet.
     - 10.0.0.16 - 10.0.0.19 ⇒ (4)
     - 10.0.0.20 - 10.0.0.23 ⇒ (4)
     - 10.0.0.24 - 10.0.0.27 ⇒ (4)
     - 10.0.0.28 - 10.0.0.31 ⇒ (4)

- **Remaining IP Addresses**:
   - There are `16,777,184` remaining IP addresses available.

```bash
 ┌─ <VPC> 10.0.0.0/8 (16,777,216) ────────────────────────────────────────────────────┐
 │                                                                                    │
 │  ┌─ <AZ-01> 10.0.0.0/29 (8) ───────────┐  ┌─ <AZ-02> 10.0.0.0/29 (8) ───────────┐  │       
 │  │                                     │  │                                     │  │          
 │  │ ┌─ <Public Subnet> ───────────────┐ │  │ ┌─ <Public Subnet> ───────────────┐ │  │               
 │  │ │       10.0.0.0 - 10.0.0.3       │ │  │ │       10.0.0.4 - 10.0.0.7       │ │  │               
 │  │ │               (4)               │ │  │ │               (4)               │ │  │       
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │     
 │  │ ┌─ <Private Subnet> ──────────────┐ │  │ ┌─ <Private Subnet> ──────────────┐ │  │        
 │  │ │      10.0.0.16 - 10.0.0.19      │ │  │ │      10.0.0.20 - 10.0.0.23      │ │  │        
 │  │ │               (4)               │ │  │ │               (4)               │ │  │        
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │      
 │  └─────────────────────────────────────┘  └─────────────────────────────────────┘  │    
 │  ┌─ <AZ-03> 10.0.0.0/29 (8) ───────────┐  ┌─ <AZ-04> 10.0.0.0/29 (8) ───────────┐  │                      
 │  │                                     │  │                                     │  │                         
 │  │ ┌─ <Public Subnet> ───────────────┐ │  │ ┌─ <Public Subnet> ───────────────┐ │  │                              
 │  │ │       10.0.0.8 - 10.0.0.11      │ │  │ │      10.0.0.12 - 10.0.0.15      │ │  │                              
 │  │ │               (4)               │ │  │ │               (4)               │ │  │                      
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │   
 │  │ ┌─ <Private Subnet> ──────────────┐ │  │ ┌─ <Private Subnet> ──────────────┐ │  │                       
 │  │ │      10.0.0.24 - 10.0.0.27      │ │  │ │      10.0.0.28 - 10.0.0.31      │ │  │      
 │  │ │               (4)               │ │  │ │               (4)               │ │  │       
 │  │ └─────────────────────────────────┘ │  │ └─────────────────────────────────┘ │  │         
 │  └─────────────────────────────────────┘  └─────────────────────────────────────┘  │           
 │                  Available IP addresses: 10.0.0.0/29 (16,777,184)                  │           
 └────────────────────────────────────────────────────────────────────────────────────┘  
```