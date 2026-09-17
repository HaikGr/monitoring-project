https://www.google.com/search?q=kind+or+minikube&oq=kind+or+minikube&gs_lcrp=EgZjaHJvbWUyBggAEEUYOTIICAEQABgWGB4yCAgCEAAYFhgeMggIAxAAGBYYHjIICAQQABgWGB4yCAgFEAAYFhgeMggIBhAAGBYYHjIICAcQABgWGB4yCAgIEAAYFhge0gEHNjkzajBqN6gCALACAA&sourceid=chrome&source=chrome.ob&ie=UTF-8

above is difference between kind and minikube, I will consider which one to use, in this case.

Your application gives you some nice extra metrics later

Don't implement these all now. Just recognize that your application naturally provides additional SRE observability opportunities.

Kafka-related metrics

You could eventually measure things like:

messages_consumed_total
typing_events_consumed_total
kafka_consumer_errors_total

and potentially consumer lag.

This would be very useful later, but Kafka is not required for M1.

PostgreSQL metrics

Because /messages writes to PostgreSQL, later you could measure:

database_operation_duration_seconds
database_errors_total
messages_created_total

Again, that's beyond the current milestone.

Application/business metrics

Your chat gives you possible domain metrics such as:

chat_messages_created_total
typing_events_total

Those are often called application/business metrics, rather than infrastructure/process metrics.



helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.retention=30d \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.accessModes[0]=ReadWriteOnce \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=20Gi \
  --set grafana.persistence.enabled=true \
  --set grafana.persistence.size=10Gi
Let me break down these critical settings:

serviceMonitorSelectorNilUsesHelmValues=false: This is crucial! It allows Prometheus to discover ServiceMonitors across all namespaces, not just ones with specific labels.
retention=30d: Keeps 30 days of metrics data
storage=20Gi: Persistent storage for Prometheus data
grafana.persistence=true: Ensures Grafana dashboards and settings survive pod restarts


# Access Prometheus (background process)
kubectl port-forward svc/prometheus-kube-prometheus-prometheus 9090:9090 -n monitoring &
# Access Grafana (background process)  
kubectl port-forward svc/prometheus-grafana 3000:80 -n monitoring &
# Access AlertManager (background process)
kubectl port-forward svc/prometheus-kube-prometheus-alertmanager 9093:9093 -n monitoring &


Useful prometheus medium
https://saraswathilakshman.medium.com/a-complete-guide-to-prometheus-grafana-and-servicemonitors-fcc104dc3087