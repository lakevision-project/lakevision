# Lakevision Kubernetes Deployment

This folder contains example Kubernetes manifests for deploying the Lakevision app (single-container, frontend + backend) and another container for health check worker.

## Files

- `deployment.yaml` - Includes the `Deployment` and `Service` for the app.
- `worker-deployment.yaml` - Includes the `Deployment` for the health check worker.
- `config-map.yaml` - Holds app configuration (env vars).
- `secrets.yaml` - Holds secrets (e.g. API keys).

## Usage

1. **Edit** the manifests to:
   - Set the correct `image:` in `deployment.yaml` and `worker-deployment.yaml` (your registry path).
   - Adjust any needed environment variables in `config-map.yaml`.
   - Update secrets in `secrets.yaml`.

2. **Apply** the manifests to your cluster:
   ```sh
   kubectl apply -f secrets.yaml
   kubectl apply -f config-map.yaml
   kubectl apply -f deployment.yaml
   kubectl apply -f worker-deployment.yaml
   ```
3. **Access** the app:

Use `kubectl get svc` to find the service, or expose it with a LoadBalancer/Route/Ingress as needed.
