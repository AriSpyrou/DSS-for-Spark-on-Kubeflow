import kfp
from kfp.components import func_to_container_op, InputPath, OutputPath
import kfp.compiler as compiler
import json
from kfp import dsl


@func_to_container_op
def print_op(message: str):
    print(message)


def spark_preprocess():
    with open('manifest_preprocess.json') as f:
      manifest_preprocess = f.read()
    
    op = kfp.dsl.ResourceOp(
      name='Spark Submit Preprocess',
      k8s_resource=json.loads(manifest_preprocess),
      action='create',
      attribute_outputs={"name": "{.metadata.name}"})
    return op.outputs

def spark_main_ml():
    with open('manifest_main_ml.json') as f:
      manifest_main_ml = f.read()
    
    op = kfp.dsl.ResourceOp(
      name='Spark Submit ML',
      k8s_resource=json.loads(manifest_main_ml),
      action='create',
      attribute_outputs={"name": "{.metadata.name}"})
    return op.outputs


@dsl.pipeline(name='spark-ml-pipeline')
def spark_pipeline():
    spark_pp_out = spark_preprocess()
    pollOp = dsl.ContainerOp(
      name='poll-spark',
      image='bitnami/kubectl:1.21.12-debian-10-r32',
      command=['bash', '-c'],
      arguments=['while true; do OUT="$(kubectl get -n spark-operator sparkapp %s -o json | jq ".status.applicationState.state")"; if [ "$OUT" = "\\"COMPLETED"\\" ]; then echo "COMPLETED" > /tmp/output.txt; break; elif [ "$OUT" = "\\"FAILED"\\" ]; then echo "FAILED" > /tmp/output.txt; break; else sleep 5; fi; done' % spark_pp_out['name']],
      file_outputs={
        'output': '/tmp/output.txt'
      }
    )
    with dsl.Condition(pollOp.outputs['output'] == 'COMPLETED'):
      spark_ml_out = spark_main_ml()
    with dsl.Condition(pollOp.output == 'FAILED'):
      print_op('Unexpected Error')
    pollOp2 = dsl.ContainerOp(
      name='poll-spark',
      image='bitnami/kubectl:1.21.12-debian-10-r32',
      command=['bash', '-c'],
      arguments=['while true; do OUT="$(kubectl get -n spark-operator sparkapp %s -o json | jq ".status.applicationState.state")"; if [ "$OUT" = "\\"COMPLETED"\\" ]; then echo "COMPLETED" > /tmp/output.txt; break; elif [ "$OUT" = "\\"FAILED"\\" ]; then echo "FAILED" > /tmp/output.txt; break; else sleep 5; fi; done' % spark_ml_out['name']],
      file_outputs={
        'output': '/tmp/output.txt'
      }
    )
    with dsl.Condition(pollOp2.output == 'COMPLETED'):
      dsl.ContainerOp(
      name='retrieve-logs-on-success',
      image='bitnami/kubectl:1.21.12-debian-10-r32',
      command=['bash', '-c'],
      arguments=['kubectl logs %s-driver -n spark-operator > /tmp/output.txt' % spark_ml_out['name']],
      file_outputs={
        'output': '/tmp/output.txt'
      }
    )
    with dsl.Condition(pollOp2.output == 'FAILED'):
      print_op('Unexpected Error')

# This is a hacky solution
authservice_session_cookie="""\
authservice_session=MTY2MjEzNjU3OHxOd3dBTkVSSFRGTldNME5QVjFOVFNFbE5SazlTVFZKTFVFRTNTa1V6VkVFelJ6SllSakpZUmxaSVVVeFhVRkZVVmpST1JrZFhORkU9fIdE1wcyORsCGZeQJNgdEK0aKfxygnYGVPaZejJPuym4\
"""
client = kfp.Client(host="http://10.64.140.43.nip.io/pipeline", 
                    cookies=authservice_session_cookie)
client.create_run_from_pipeline_func(
    spark_pipeline,
    arguments={},
    experiment_name='Spark-Experiment',
    namespace='admin')

#TODO Necessary: Check if spark program is done before continuing with the use of polling
#TODO Find out how to disassemble the final column before assembling it right before running the ML
#     check: https://stackoverflow.com/questions/44160768/spark-2-1-cannot-write-vector-field-on-csv 