# Assignment-1: Cloud-Native Architecture Design
NutBodyslam053@TCC231019

## Network Infrastructure
```bash
                                                     🌐
                                           << Internet Gateway >>
                                                      ↑
                                                     🌡️
                                       << Application Load Balancer >>
                                                      ↑
┌─ <Region: East Asia> ───────────────────────────┐   ┊   ┌─ <Region: Southeast Asia> ──────────────────────┐  
│                                                 │   ┊   │                                                 │   
│  ┌─ <VPC> ───────────────────────────────────┐  │   ┊   │  ┌─ <VPC> ───────────────────────────────────┐  │      
│  │                                           │  │   ┊   │  │                                           │  │     
│  │  ┌─ <AZ-A> ────────────────────────────┐  │  │   ┊   │  │  ┌─ <AZ-C> ────────────────────────────┐  │  │    
│  │  │                                     │  │  │   ┊   │  │  │                                     │  │  │   
│  │  │ ┌─ <Private Subnet> ──────────────┐ │  │  │   ┊   │  │  │ ┌─ <Private Subnet> ──────────────┐ │  │  │        
│  │  │ │                                 │ │  │  │   ┊   │  │  │ │                                 │ │  │  │        
│  │  │ │      ['Client Application']     │ │  │  │   ┊   │  │  │ │      ['Client Application']     │ │  │  │     
│  │  │ └─────────────────────────────────┘ │  │  │   ┊   │  │  │ └─────────────────────────────────┘ │  │  │     
│  │  │                   ↓                 │  │  │   ┊   │  │  │                   ↓                 │  │  │    
│  │  │           << Route Table >>         │  │  │   ┊   │  │  │           << Route Table >>         │  │  │     
│  │  │                   ↓                 │  │  │   ┊   │  │  │                   ↓                 │  │  │           
│  │  │           << NAT Gateway >>         │  │  │   ┊   │  │  │           << NAT Gateway >>         │  │  │      
│  │  │                   ┊                 │  │  │   ┊   │  │  │                   ┊                 │  │  │        
│  │  │ ┌─ <Public Subnet> ───────────────┐ │  │  │   ┊   │  │  │ ┌─ <Public Subnet> ───────────────┐ │  │  │      
│  │  │ │                                 │ │  │  │   ┊   │  │  │ │                                 │ │  │  │    
│  │  │ │    ['Server']   ['Database']    │ │  │  │   ┊   │  │  │ │    ['Server']   ['Database']    │ │  │  │      
│  │  │ └─────────────────────────────────┘ │  │  │   ┊   │  │  │ └─────────────────────────────────┘ │  │  │         
│  │  └───────────────────┼─────────────────┘  │  │   ┊   │  │  └───────────────────┼─────────────────┘  │  │       
│  │                      ↓                    │  │   ┊   │  │                      ↓                    │  │        
│  │              << Route Table >> ┄┄┄┄┄┄┄┄┄┄┄┼┄┄┼┄┄┄┴┄┄┄┼┄┄┼┄┄┄┄┄┄┄┄┄┄┄┄┄ << Route Table >>            │  │    
│  │                      ↑                    │  │       │  │                      ↑                    │  │   
│  │  ┌─ <AZ-B> ──────────┼─────────────────┐  │  │       │  │  ┌─ <AZ-D> ──────────┼─────────────────┐  │  │          
│  │  │                   ┊                 │  │  │       │  │  │                   ┊                 │  │  │        
│  │  │ ┌─ <Public Subnet> ───────────────┐ │  │  │       │  │  │ ┌─ <Public Subnet> ───────────────┐ │  │  │      
│  │  │ │                                 │ │  │  │       │  │  │ │                                 │ │  │  │         
│  │  │ │      ['Client Application']     │ │  │  │       │  │  │ │      ['Client Application']     │ │  │  │             
│  │  │ └─────────────────┬───────────────┘ │  │  │       │  │  │ └─────────────────┬───────────────┘ │  │  │    
│  │  │                   ┊                 │  │  │       │  │  │                   ┊                 │  │  │   
│  │  │           << NAT Gateway >>         │  │  │       │  │  │           << NAT Gateway >>         │  │  │    
│  │  │                   ↑                 │  │  │       │  │  │                   ↑                 │  │  │     
│  │  │           << Route Table >>         │  │  │       │  │  │           << Route Table >>         │  │  │      
│  │  │                   ↑                 │  │  │       │  │  │                   ↑                 │  │  │      
│  │  │ ┌─ <Private Subnet> ──────────────┐ │  │  │       │  │  │ ┌─ <Private Subnet> ──────────────┐ │  │  │        
│  │  │ │                                 │ │  │  │       │  │  │ │                                 │ │  │  │ 
│  │  │ │    ['Server']   ['Database']    │ │  │  │       │  │  │ │    ['Server']   ['Database']    │ │  │  │  
│  │  │ └─────────────────────────────────┘ │  │  │       │  │  │ └─────────────────────────────────┘ │  │  │    
│  │  └─────────────────────────────────────┘  │  │       │  │  └─────────────────────────────────────┘  │  │      
│  │         {Database Synchronization}        │  │       │  │         {Database Synchronization}        │  │          
│  └───────────────────────────────────────────┘  │       │  └───────────────────────────────────────────┘  │  
│                                                 │       │                                                 │  
└─────────────────────────┬───────────────────────┘       └─────────────────────────┬───────────────────────┘    
                          ┊                                                         ┊                         
                          └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄ << VPC Peering >> ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘
                                                     🔌
```

## The Characteristics Of Cloud-Native Architecture

1. **High Level of Automation:** Cloud-native architecture relies heavily on automation for tasks such as provisioning, scaling, and monitoring of resources. This reduces manual intervention, improves efficiency, and ensures consistent deployment and management processes.

2. **Self-Healing:** Cloud-native applications are designed to be resilient. They can detect and recover from failures automatically, minimizing downtime and ensuring continuous availability. Self-healing mechanisms are essential for maintaining a robust and reliable system.

3. **Scalable:** Cloud-native applications can scale both vertically (adding more resources to a single component) and horizontally (adding more instances of components) based on demand. This elasticity allows them to handle varying workloads effectively.

4. **Cost-Efficient:** Cloud-native architecture optimizes resource utilization, enabling organizations to pay only for the resources they consume. It also allows for dynamic scaling, which means resources can be added or removed as needed, reducing unnecessary expenses.

5. **Easy to Maintain:** Cloud-native applications are built using microservices and containers, which promote modular design. This makes it easier to update, maintain, and troubleshoot individual components without affecting the entire system. DevOps practices and automation tools further streamline maintenance.

6. **Secure by Default:** Security is a fundamental aspect of cloud-native architecture. It emphasizes security measures at every layer, from infrastructure to application code. Features like identity and access management, encryption, and regular security testing are integrated into the architecture from the outset.