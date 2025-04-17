#!/bin/bash

scale_wait() {
  local name=$1
  local replicas=$2
  local ns=$3
  kubectl scale statefulset $name --replicas=$replicas -n $ns
  while ! kubectl get sts $name -n $ns | grep "$replicas/$replicas"; do
    echo "waiting for $name to be scaled to $replicas"
    sleep 10
  done
}

patch_crd() {
  local crd_name=$1
  local ns=$2
  if kubectl get "crd/${crd_name}" -o jsonpath='{.metadata.labels.app\.kubernetes\.io/managed-by}' | grep -q '^Helm'; then
    kubectl label "crd/${crd_name}" app.kubernetes.io/managed-by=Helm --overwrite
    kubectl annotate "crd/${crd_name}" meta.helm.sh/release-name=dr   --overwrite
    kubectl annotate "crd/${crd_name}" meta.helm.sh/release-namespace=$ns --overwrite
  fi
}


set -x
export NS="${NS:-upgrade1}"
start_time=$(date +%s)


patch_crd "lrs.lrs.datarobot.com" $NS
patch_crd "executionenvironments.predictions.datarobot.com" $NS
patch_crd "inferenceservers.predictions.datarobot.com" $NS
patch_crd "notebooks.notebook.datarobot.com" $NS
patch_crd "notebookvolumes.notebook.datarobot.com" $NS
patch_crd "notebookvolumesnapshots.notebook.datarobot.com" $NS

# power off DR
kubectl scale statefulset -l app.kubernetes.io/instance=dr --replicas=0 -n $NS
kubectl scale deployment -l app.kubernetes.io/instance=dr --replicas=0 -n $NS
# power off PCS
kubectl scale statefulset -l app.kubernetes.io/instance=pcs --replicas=0 -n $NS
kubectl scale deployment -l app.kubernetes.io/instance=pcs --replicas=0 -n $NS

# change labels from PCS to DR
for kind in secret pvc networkpolicy serviceaccount configmap service role rolebinding pdb; do
    for sts in $(kubectl get $kind -l app.kubernetes.io/instance=pcs -n $NS -o jsonpath='{.items[*].metadata.name}'); do
        echo "retag $kind/$sts"
        kubectl label $kind $sts app.kubernetes.io/instance=dr -n $NS --overwrite
        kubectl annotate $kind $sts meta.helm.sh/release-name=dr -n $NS --overwrite
    done
done

# List all PVCs in the specified namespace
kubectl get pvc -n $NS -o jsonpath='{.items[*].metadata.name}' | tr ' ' '\n' | while read pvc; do
    # Get the PV associated with the PVC
    pv=$(kubectl get pvc $pvc -n $NS -o jsonpath='{.spec.volumeName}')

    # Get the persistentVolumeReclaimPolicy for the PV
    reclaimPolicy=$(kubectl get pv $pv -o jsonpath='{.spec.persistentVolumeReclaimPolicy}')

    if [[ $pvc == *"pcs"* ]]; then
        # Print the PVC name and its associated PV's reclaim policy
        # echo "PVC: $pvc, PV: $pv, Reclaim Policy: $reclaimPolicy"
        if [[ $reclaimPolicy != "Retain" ]]; then
            # Print the PVC name and its associated PV's reclaim policy
            echo "patch PV: $pv to Retain"
            kubectl patch pv $pv -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
        fi
    fi
done



scale_wait pcs-mongo 3 $NS

export MONGODB_ROOT_USER="pcs-mongodb"
export MONGODB_OLD_ROOT_PASSWORD=$(kubectl get secret --namespace $NS pcs-mongo -o jsonpath="{.data.mongodb-root-password}" | base64 -d)
if [[ ${#MONGODB_OLD_ROOT_PASSWORD} -lt 18 ]]; then
  echo "- fix mongo secret"
  MONGODB_NEW_ROOT_PASSWORD=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 18 )
  kubectl exec -i -t -n $NS pcs-mongo-0 -- bash -c "mongosh --username $MONGODB_ROOT_USER --password $MONGODB_OLD_ROOT_PASSWORD --host pcs-mongo-headless --authenticationDatabase admin  --eval \"use admin;\" --eval \"db.changeUserPassword('$MONGODB_ROOT_USER', '$MONGODB_NEW_ROOT_PASSWORD')\" "
  kubectl patch secret pcs-mongo -n $NS -p "{\"stringData\":{\"mongodb-root-password\":\"$MONGODB_NEW_ROOT_PASSWORD\"}}"
else
  MONGODB_NEW_ROOT_PASSWORD=$MONGODB_OLD_ROOT_PASSWORD
fi

mongodb_image=$(kubectl get statefulset pcs-mongo -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $1}')
mongodb_image_tag=$(kubectl get statefulset pcs-mongo -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $2}')
if [[ $mongodb_image_tag == 5* ]]; then
  echo "Image tag begins with 5"
  echo "- set FCV to 6.0"

  if [[ -z "$DOCKERHUB_USERNAME" || -z "$DOCKERHUB_PASSWORD" ]]; then
    echo "Error: DOCKERHUB_USERNAME or DOCKERHUB_PASSWORD is not set."
    echo "using mirror_chainguard_datarobot.com_mongodb-bitnami-fips:6.0"
    base_repo=$(dirname $mongodb_image)
    echo "using $base_repo/mirror_chainguard_datarobot.com_mongodb-bitnami-fips:6.0"
    kubectl patch statefulset pcs-mongo -n $NS --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"'$base_repo'/mirror_chainguard_datarobot.com_mongodb-bitnami-fips:6.0"}]'
  else
    echo "Creating image pull secret for DockerHub..."
    kubectl create secret docker-registry tmp-image-pullsecret -n $NS \
      --docker-username="$DOCKERHUB_USERNAME" \
      --docker-password="$DOCKERHUB_PASSWORD" \
      --docker-server="https://index.docker.io/v1/"
    kubectl patch serviceaccount pcs-mongodb-sa -n $NS -p '{"imagePullSecrets": [{"name": "tmp-image-pullsecret"}]}'
    kubectl patch statefulset pcs-mongo -n $NS --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"docker.io/datarobot/mirror_chainguard_datarobot.com_mongodb-bitnami-fips:6.0"}]'
    sleep 10
    scale_wait pcs-mongo 3 $NS
  fi
  kubectl exec -i -t -n $NS pcs-mongo-0 -- bash -c "mongosh --eval \"db.adminCommand({ getParameter: 1, featureCompatibilityVersion: 1 })\" --username $MONGODB_ROOT_USER --password $MONGODB_NEW_ROOT_PASSWORD --authenticationDatabase admin --host pcs-mongo-headless"
  kubectl exec -i -t -n $NS pcs-mongo-0 -- bash -c "mongosh --eval \"db.adminCommand({ setFeatureCompatibilityVersion: '6.0' })\" --username $MONGODB_ROOT_USER --password $MONGODB_NEW_ROOT_PASSWORD --authenticationDatabase admin --host pcs-mongo-headless"
else
  echo "Image tag does not begin with 5"
fi


kubectl delete secret tmp-image-pullsecret -n $NS | true

echo "- fix rabbitmq secret"
NEWSECRET=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 18 | base64)
kubectl patch secret pcs-rabbitmq -n $NS -p "{\"data\":{\"rabbitmq-password\":\"$NEWSECRET\"}}"

echo "- fix redis secret"
NEWSECRET=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 18 | base64)
kubectl patch secret pcs-redis -n $NS -p "{\"data\":{\"redis-password\":\"$NEWSECRET\"}}"

echo "- fix elasticsearch secret"
NEWSECRET=$(openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 18 | base64)
kubectl patch secret pcs-elasticsearch -n $NS -p "{\"data\":{\"elasticsearch-password\":\"$NEWSECRET\"}}"

rabbitmq_image_tag=$(kubectl get statefulset pcs-rabbitmq -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $2}')
if [[ $rabbitmq_image_tag == 3.12* ]]; then
  echo "Image tag begins with 3.12 - no direct upgrade to 4.0.5"
  kubectl scale statefulset pcs-rabbitmq --replicas=0 -n $NS
  kubectl patch statefulset pcs-rabbitmq -n $NS --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/containers/0/image", "value":"docker.io/bitnami/rabbitmq:3.13.7"}]'
  kubectl patch statefulset pcs-rabbitmq -n $NS --type='json' -p='[{"op": "replace", "path": "/spec/template/spec/initContainers/0/image", "value":"docker.io/bitnami/rabbitmq:3.13.7"}]'
  kubectl scale statefulset pcs-rabbitmq --replicas=1 -n $NS
fi
FF=$(kubectl exec -i -t -n $NS pcs-rabbitmq-0 -c rabbitmq -- bash -c "rabbitmqctl -q list_feature_flags | grep stream_filtering" | awk '{print $2}')
if [[ $FF == "enabled" ]]; then
  echo "Feature flag stream_filtering is enabled"
else
  echo "Feature flag stream_filtering is not enabled"
  kubectl exec -i -t -n $NS pcs-rabbitmq-0 -c rabbitmq -- bash -c "rabbitmqctl set_feature_flag stream_filtering true"
fi

scale_wait pcs-postgresql 3 $NS

if kubectl get configmap pcs-postgresql-configuration -n $NS > /dev/null 2>&1; then
    echo "ConfigMap pcs-postgresql-configuration exists."
    # Check if wal_keep_segments is in the ConfigMap
    if kubectl get configmap pcs-postgresql-configuration -n $NS -o yaml | grep -q wal_keep_segments; then
        echo "remove 'wal_keep_segments' from ConfigMap."
        kubectl get configmap pcs-postgresql-configuration -n $NS -o yaml | sed '/wal_keep_segments/d' | kubectl apply -n $NS -f -
        kubectl rollout restart sts pcs-postgresql -n $NS
    fi
fi


RUN_MAGRATION="false"
if kubectl get statefulset pcs-postgresql -n $NS > /dev/null 2>&1; then

  postgresql_image_tag=$(kubectl get statefulset pcs-postgresql -n $NS -o jsonpath='{.spec.template.spec.containers[0].image}' | awk -F: '{print $2}')
  if [[ $postgresql_image_tag == 12* ]]; then
  RUN_MAGRATION="true"
  echo "StatefulSet pcs-postgresql exists and has version 12"

  PRIMARY=$(kubectl exec -i -t -n $NS pcs-postgresql-0 -c postgresql -- bash -c "/opt/bitnami/scripts/postgresql-repmgr/entrypoint.sh repmgr cluster show -f /opt/bitnami/repmgr/conf/repmgr.conf --compact" | grep primary  | grep running | awk '{print $3}')
  echo "- forcing primary to be pcs-postgresql-0"
  while [[ "$PRIMARY" != "pcs-postgresql-0" ]]; do
    echo "stopping $PRIMARY"
    kubectl exec -i -t -n $NS $PRIMARY -- bash -c "/opt/bitnami/postgresql/bin/pg_ctl -D /bitnami/postgresql/data -m fast stop"
    sleep 10
    PRIMARY=$(kubectl exec -i -t -n $NS pcs-postgresql-0 -c postgresql -- bash -c "/opt/bitnami/scripts/postgresql-repmgr/entrypoint.sh repmgr cluster show -f /opt/bitnami/repmgr/conf/repmgr.conf --compact" | grep primary  | grep running | awk '{print $3}')
  done

  echo "- drop modmon functions"
  kubectl exec -i -t -n $NS pcs-postgresql-0 -- sh -c 'export PGPASSWORD="$POSTGRES_POSTGRES_PASSWORD"; psql -U postgres -d modmon <<EOF
DROP FUNCTION IF EXISTS validate_bh_tt_count_less_than(
        actual_value_count integer,
        unique_value_count integer,
        aggregate_record_count integer,
        min_value double precision,
        max_value double precision,
        thresholds double precision[],
        random_seed double precision );
DROP FUNCTION IF EXISTS validate_bh_tt_percentiles(
        actual_value_count integer,
        unique_value_count integer,
        aggregate_record_count integer,
        min_value double precision,
        max_value double precision,
        percentiles double precision[],
        random_seed double precision);

DROP AGGREGATE IF EXISTS array_cat_agg(anyarray);
EOF'
kubectl exec -i -t -n "$NS" pcs-postgresql-0 -- sh -c 'export PGPASSWORD="$POSTGRES_POSTGRES_PASSWORD"; psql -U postgres -d modmon -t <<EOF
SELECT '\''validate_bh_tt_count_less_than'\'' AS object,
       CASE WHEN EXISTS (
           SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
           WHERE p.proname = '\''validate_bh_tt_count_less_than'\''
       ) THEN '\''EXISTS'\'' ELSE '\''DOES NOT EXIST'\'' END;
SELECT '\''validate_bh_tt_percentiles'\'' AS object,
       CASE WHEN EXISTS (
           SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
           WHERE p.proname = '\''validate_bh_tt_percentiles'\''
       ) THEN '\''EXISTS'\'' ELSE '\''DOES NOT EXIST'\'' END;
SELECT '\''array_cat_agg'\'' AS object,
       CASE WHEN EXISTS (
           SELECT 1 FROM pg_aggregate a
           JOIN pg_proc p ON a.aggfnoid = p.oid
           WHERE p.proname = '\''array_cat_agg'\''
       ) THEN '\''EXISTS'\'' ELSE '\''DOES NOT EXIST'\'' END;
EOF'
  fi
  kubectl exec -i -t -n $NS pcs-postgresql-0 -c postgresql -- bash -c "/opt/bitnami/scripts/postgresql-repmgr/entrypoint.sh repmgr cluster show -f /opt/bitnami/repmgr/conf/repmgr.conf --compact"
fi

echo "- delete PCS statefulset and deployment (helm upgrade is going to rebuilt it)"
kubectl delete statefulset -l app.kubernetes.io/instance=pcs -n $NS
kubectl delete deployment -l app.kubernetes.io/instance=pcs -n $NS
kubectl delete pvc data-pcs-rabbitmq-0 -n $NS


echo "- delete old PCS secrets"
for sec in pcs-db-buildservice pcs-db-cspspark pcs-db-identityresourceservice pcs-db-messagequeue pcs-db-modmon pcs-db-predenv pcs-db-sushihydra pcs-pgpool pcs-pgpool-custom-users pcs-pgppol-userdb pcs-postgresql-initdb pcs-postgresql-initdb-cfg pcs-redis; do
  kubectl delete secret $sec -n $NS
done




if kubectl get deployment auth-server-hydra -n $NS -o jsonpath='{.metadata.labels.helm\.sh/chart}' | grep -q '^hydra-'; then
    # If the label exists, delete the deployment
    echo "Deleted legacy auth-server-hydra"
    kubectl delete deployment auth-server-hydra -n $NS
fi


helm upgrade --install dr oci://registry-1.docker.io/datarobotdev/datarobot-prime --version 11.0.0-rc11 --debug -n $NS -f ./pcs.yaml -f ./dr-11p0.yaml --set pg-upgrade.enabled=$RUN_MAGRATION --timeout 30m


for secrets in $(kubectl get secret -l owner=helm -l name=pcs -n $NS -o jsonpath='{.items[*].metadata.name}'); do
    kubectl delete secret $secrets -n $NS
done


end_time=$(date +%s)
elapsed_seconds=$((end_time - start_time))
elapsed_minutes=$((elapsed_seconds / 60))
remaining_seconds=$((elapsed_seconds % 60))

echo "Script executed in $elapsed_minutes minutes and $remaining_seconds seconds"

# cleanup
# helm uninstall dr -n $NS
# kubectl delete ns $NS

