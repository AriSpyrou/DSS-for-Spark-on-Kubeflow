import kfp
from kfp.components import func_to_container_op, InputPath, OutputPath
import kfp.compiler as compiler
import json
from kfp import dsl


def spark_preprocess():
    with open('manifest_preprocess.json') as f:
      manifest_preprocess = f.read()
    
    op = kfp.dsl.ResourceOp(
      name='Spark Dataset Preprocess',
      k8s_resource=json.loads(manifest_preprocess),
      action='create',
      attribute_outputs={"name": "{.metadata.name}"})
    return op.outputs

@func_to_container_op
def spark_main_ml(pod_name: str):
    with open('manifest_main_ml.json') as f:
      manifest_main_ml = f.read()
    print(pod_name)
    op = kfp.dsl.ResourceOp(
      name='Spark Dataset Machine Learning',
      k8s_resource=json.loads(manifest_main_ml),
      action='create',
      attribute_outputs={"name": "{.metadata.name}"})
    return op.outputs

@dsl.pipeline(name='spark-ml-pipeline')
def spark_pipeline():
    out = spark_preprocess()
    spark_main_ml(out['name'])

# This is a hacky solution
authservice_session_cookie="""\
authservice_session=MTY2MjA0OTgwMXxOd3dBTkZSQlZFVXlOV\
VJLUXpReVdEWkNUa1ZJVVVwVFFqWklOMFphUTFaSVNqWTJURWhIVX\
paS1YxVlpVMHRXU1VOTlJVSkxSRUU9fG9ysBIUzpBzkbN_ErxOafh2t6b0ZhfEryYkM-xj91Uv\
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