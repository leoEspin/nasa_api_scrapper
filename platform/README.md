# Platform Engineering Interview Guidelines

Welcome to the Platform Engineer (Site Reliability Engineer) take-home coding challenge! 

This project is designed to evaluate your skills in productionizing applications, managing container orchestration, and ensuring the reliability and observability of services in a Kubernetes environment.

## Overview:

In this repository, we have included two distinct projects:
- [Backend Service](../backend/README.md) - Backend Application (BE)
- [Frontend Application](../frontend/README.md) - Frontend Application (FE)

Your task is to choose either one of these projects and productionize them.

The goal is to create a containerized deployment strategy using Docker and deploy the application(s) on Kubernetes (K8s) with Helm charts or other K8s resources.

This assignment will give us insight into your technical expertise, problem-solving skills, and ability to work with modern infrastructure tools.

#### Fork the repository and clone it locally
- https://github.com/Tekmetric/interview.git

#### Import project into IDE
- Project root is located in `platform` folder

#### After finishing the goals listed below create a PR

### Goals
1. Dockerization: Create Dockerfiles to containerize the chosen application(s).
Kubernetes Deployment:
2. Develop Helm charts or standard Kubernetes manifests to deploy the application(s) on a K8s cluster.
3. Application Enhancements: Ensure each application has appropriate health checks (e.g., liveness and readiness probes).
Add relevant metrics (e.g., Prometheus instrumentation) to monitor application performance.
4. Documentation: Provide clear instructions on how to build and deploy the application(s), including any prerequisites.

### Considerations
This is an open-ended exercise for you to showcase what you know!

### Submitting your coding exercise
Once you have finished the coding exercise please create a PR into Tekmetric/interview

### Excited to see what you build! 🚀