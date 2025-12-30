## 📋 Overview
SAFE-LIB is an ontology based framework for anomaly detection in lithium-ion batteries (LIBs) that combines heterogeneous data sources including time series sensor measurements and thermal imaging—within a modular semantic reasoning architecture. By integrating stream reasoning with domain knowledge representation, SAFE-LIB enables early detection of safety-critical events such as overheating, thermal imbalance, and other battery anomalies.
This work addresses key limitations in current battery monitoring approaches by:

1-Unifying multimodal data (electrical sensors + thermal imaging) in a single semantic framework
2-Providing interpretable explanations through ontology-based reasoning
3-Enabling real-time detection via C-SPARQL stream reasoning
4-Supporting Industry 5.0 principles through human centric, transparent AI

## 🏗️ Architecture


┌─────────────────────────────────────────────────────────────┐
│                       Data Layer                            │
│  ┌──────────────┐              ┌─────────────────────────┐  │ 
│  │ Time-Series  │              │   Thermal Imaging       │  │
│  │   Sensors    │              │   + Segmentation        │  │
│  └──────────────┘              └─────────────────────────┘  │
└────────────────────┬────────────────────┬───────────────────┘
                     │                    │
                     ▼                    ▼
          ┌──────────────────────────────────────────┐
          │    Semantic Translation & RDF Streams    │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │     Battery Anomaly Ontology (BAO)       │
          │  ┌────────────────────────────────────┐  │
          │  │ BattINFO │ Sensor │ Time │ Location│  │
          │  │   Image  │ Anomaly                 │  │
          │  └────────────────────────────────────┘  │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │    Stream Reasoning Layer (C-SPARQL)     │
          └──────────────────┬───────────────────────┘
                             │
                             ▼
          ┌──────────────────────────────────────────┐
          │      Anomaly Detection & Explanation     │
          └──────────────────────────────────────────┘



## Getting Started
Prerequisites

Python 3.8 or higher
Java 8+ (required for C-SPARQL engine)
Git


## Installation

git clone https://github.com/Marwa-Zitouni/SAFE-LIB.git
cd SAFE-LIB


