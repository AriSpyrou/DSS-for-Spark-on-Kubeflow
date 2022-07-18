import kfp
from kfp.components import func_to_container_op, InputPath, OutputPath
import json
from kfp import dsl
_CONTAINER_MANIFEST = """
{
  "apiVersion": "sparkoperator.k8s.io/v1beta2",
  "kind": "SparkApplication",
  "metadata": {
    "generateName": "pyspark-pi-",
    "namespace": "spark-operator"
  },
  "spec": {
    "type": "Python",
    "pythonVersion": "3",
    "mode": "cluster",
    "image": "gcr.io/spark-operator/spark-py:v3.1.1",
    "imagePullPolicy": "Always",
    "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
    "sparkVersion": "3.1.1",
    "restartPolicy": {
      "type": "OnFailure",
      "onFailureRetries": 3,
      "onFailureRetryInterval": 10,
      "onSubmissionFailureRetries": 5,
      "onSubmissionFailureRetryInterval": 20
    },
    "driver": {
      "cores": 1,
      "coreLimit": "1200m",
      "memory": "512m",
      "labels": {
        "version": "3.1.1"
      },
      "serviceAccount": "spark-spark"
    },
    "executor": {
      "cores": 1,
      "instances": 1,
      "memory": "512m",
      "labels": {
        "version": "3.1.1"
      }
    }
  }
}
"""

@func_to_container_op
def print_op(message: str):
    print(message)

def resourceop_basic():
    op = kfp.dsl.ResourceOp(
      name='Start Spark on K8s',
      k8s_resource=json.loads(_CONTAINER_MANIFEST),
      action='create',
      attribute_outputs={"name": "{.metadata.name}"})
    return op.outputs

@dsl.pipeline(name='my-pipeline')
def my_pipeline():
    create_resource_task = resourceop_basic()
    print_op(create_resource_task['name'])

# This is a hacky solution
authservice_session_cookie="""\
authservice_session=MTY1ODE1MDczM3xOd3dBTkZaVVNsVkZSVWRSVlRK\
VVQwZFVWMVZUU3pSTVVqSlNXRmhTVURNM1MwWklVRkZEVkRkUU5VZGFRMDR5\
TWxSRlNWY3pVVkU9fGTWhvfUsgy_y2keLVzT_ysFhTyQEAndDCjE94RFGU7l\
"""
client = kfp.Client(host="http://10.64.140.43.nip.io/pipeline", 
                    cookies=authservice_session_cookie)
client.create_run_from_pipeline_func(
    my_pipeline,
    arguments={},
    experiment_name='Test-Exp',
    namespace='admin')
