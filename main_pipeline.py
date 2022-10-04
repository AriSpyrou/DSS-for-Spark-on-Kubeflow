import json
import kfp
from kfp.components import func_to_container_op
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
    poll_op = dsl.ContainerOp(
      name='poll-spark-preprocess-driver',
      image='bitnami/kubectl:1.21.12-debian-10-r32',
      command=['bash', '-c'],
      arguments=[f'while true; \
                 do OUT="$(kubectl get -n spark-operator sparkapp {spark_pp_out["name"]} -o json | jq ".status.applicationState.state")"; \
                 if [ "$OUT" = "\\"COMPLETED"\\" ]; \
                 then echo "COMPLETED" > /tmp/output.txt; break; \
                 elif [ "$OUT" = "\\"FAILED"\\" ]; then echo "FAILED" > /tmp/output.txt; break; \
                 else sleep 5; fi; done'],
      file_outputs={
        'output': '/tmp/output.txt'
      }
    )
    with dsl.Condition(poll_op.outputs['output'] == 'COMPLETED'):
        spark_ml_out = spark_main_ml()
    with dsl.Condition(poll_op.output == 'FAILED', name='print-on-failure'):
        print_op('Unexpected Error')
    poll_op_2 = dsl.ContainerOp(
      name='poll-spark-ml-driver',
      image='bitnami/kubectl:1.21.12-debian-10-r32',
      command=['bash', '-c'],
      arguments=[f'while true; \
                 do OUT="$(kubectl get -n spark-operator sparkapp {spark_ml_out["name"]} -o json | jq ".status.applicationState.state")"; \
                 if [ "$OUT" = "\\"COMPLETED"\\" ]; \
                 then echo "COMPLETED" > /tmp/output.txt; break; \
                 elif [ "$OUT" = "\\"FAILED"\\" ]; then echo "FAILED" > /tmp/output.txt; break; \
                 else sleep 5; fi; done'],
      file_outputs={
        'output': '/tmp/output.txt'
      }
    )
    with dsl.Condition(poll_op_2.output == 'COMPLETED'):
        dsl.ContainerOp(
        name='retrieve-logs-on-success',
        image='bitnami/kubectl:1.21.12-debian-10-r32',
        command=['bash', '-c'],
        arguments=[f'kubectl logs {spark_ml_out["name"]}-driver -n spark-operator \
                    | tee /tmp/output.txt'],
        file_outputs={
        'output': '/tmp/output.txt'
        }
    )
    with dsl.Condition(poll_op_2.output == 'FAILED', name='print-on-failure'):
        print_op('Unexpected Error')

# This is a hacky solution
AUTHSERVICE_SESSION_COOKIE="""\
authservice_session=MTY2NDg4ODE3MHxOd3dBTkVGRE4xQldVbFZUUlVoWFUxZFhSbGhMVEZoVFYxUldWazVWUjBOU04xUkNRMGhQTTFRM1ZGWkhOVk5EV0ZkTlNEZFZURUU9fK2wf6Px1r3k6luZodP5pitAEFDupmNp4L2hnR2iMuEq\
"""
client = kfp.Client(host="http://10.64.140.43.nip.io/pipeline", 
                    cookies=AUTHSERVICE_SESSION_COOKIE)
client.create_run_from_pipeline_func(
    spark_pipeline,
    arguments={},
    experiment_name='Spark-Experiment',
    namespace='admin')
    