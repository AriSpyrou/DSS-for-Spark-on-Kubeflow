import kfp
from kfp.components import func_to_container_op, InputPath, OutputPath
import kfp.compiler as compiler
import json
from kfp import dsl

@func_to_container_op
def print_op(message: str):
    print(message)

def resourceop_basic():
    with open('spark-py-pi.json') as f:
      _CONTAINER_MANIFEST = f.read()
    
    op = kfp.dsl.ResourceOp(
      name='Start Spark on K8s',
      k8s_resource=json.loads(_CONTAINER_MANIFEST),
      action='create',
      attribute_outputs={"name": "{.metadata.name}"})
    return op.outputs

def get_logs(pod_name: str):
  op = kfp.dsl.ResourceOp(
    name='Retrieve Logs from Spark Driver',
    k8s_resource=f'{pod_name} -n spark-operator',
    action='logs'
  )
# TODO Find how to get logs from a pod. Maybe use the API to get them directly. Alt: Try with get(???) Final: Write logs in file in hdfs
@dsl.pipeline(name='my-pipeline')
def my_pipeline():
    create_resource_task = resourceop_basic()
    get_logs(create_resource_task['name'])
    #print_op(create_resource_task['name'])

# This is a hacky solution
authservice_session_cookie="""\
authservice_session=MTY2MTk2MjIyOXxOd3dBTkVKYVdFNU9XVlZT\
V2pKRldVcE9TVWxCTWxoT1UwWklSazFYVVVWSlYwMDJURlJGVmxFMVNs\
RlpOVnBNVEVzelNWWkdUVUU9fMZOn3odJnp_-VWKNCUCIXQ7f-xIijvyAeJJYEMtPjLa\
"""
client = kfp.Client(host="http://10.64.140.43.nip.io/pipeline", 
                    cookies=authservice_session_cookie)
client.create_run_from_pipeline_func(
    my_pipeline,
    arguments={},
    experiment_name='Test-Exp',
    namespace='admin')

# Alternative
# compiler.Compiler().compile(my_pipeline, "paparia.yaml")