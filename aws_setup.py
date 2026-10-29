"""Create / test / remove the AWS cloud layer (DynamoDB + Lambda + Function URL).

Needs AWS credentials configured (`aws configure`) and `pip install boto3`.
Everything used here fits in the AWS free tier at this project's volumes.

  python aws_setup.py --deploy                 # table + IAM role + Lambda + HTTPS URL
  python aws_setup.py --update                 # re-upload code after editing lambda/common
  python aws_setup.py --test                   # send one reading, show the reply
  python aws_setup.py --ping 20                # measure 20 round trips to AWS
  python aws_setup.py --status                 # what exists, item count
  python aws_setup.py --teardown               # delete everything created above
  python aws_setup.py --deploy --region ap-south-1

The endpoint is saved to aws_config.json, which simulation.py --cloud aws reads.
The Function URL uses auth NONE so plain HTTPS works from Python; delete it
(--teardown) when the experiments are over.
"""
import argparse
import io
import json
import statistics
import sys
import time
import urllib.request
import zipfile

from common import config

NAME = "icu-monitoring"
ROLE = "icu-monitoring-lambda-role"
BUNDLE = ["lambda_function.py", "common/__init__.py", "common/detection.py", "common/cloud_core.py"]


def clients(region):
    try:
        import boto3
    except ImportError:
        sys.exit("pip install boto3")
    s = boto3.session.Session(region_name=region)
    return s.client("dynamodb"), s.client("iam"), s.client("lambda"), s.client("sts")


def bundle() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for rel in BUNDLE:
            z.write(config.ROOT / rel, rel)
    return buf.getvalue()


def deploy(region):
    ddb, iam, lam, sts = clients(region)
    account = sts.get_caller_identity()["Account"]

    try:
        ddb.create_table(TableName=NAME, BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"},
                                               {"AttributeName": "sk", "AttributeType": "S"}],
                         KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                                    {"AttributeName": "sk", "KeyType": "RANGE"}])
        print("creating DynamoDB table ...")
        ddb.get_waiter("table_exists").wait(TableName=NAME)
    except ddb.exceptions.ResourceInUseException:
        print("DynamoDB table exists")
    table_arn = f"arn:aws:dynamodb:{region}:{account}:table/{NAME}"

    trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                                      "Principal": {"Service": "lambda.amazonaws.com"}}]}
    try:
        role_arn = iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        print("created IAM role; waiting for it to propagate ...")
        time.sleep(10)
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=ROLE)["Role"]["Arn"]
    iam.attach_role_policy(RoleName=ROLE,
                           PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    iam.put_role_policy(RoleName=ROLE, PolicyName="dynamodb-write", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Resource": table_arn,
                       "Action": ["dynamodb:PutItem", "dynamodb:BatchWriteItem", "dynamodb:Query",
                                  "dynamodb:GetItem"]}]}))

    code = bundle()
    for attempt in range(6):   # a brand-new role can take a few seconds to become assumable
        try:
            lam.create_function(FunctionName=NAME, Runtime="python3.12", Role=role_arn,
                                Handler="lambda_function.lambda_handler", Code={"ZipFile": code},
                                Timeout=10, MemorySize=256,
                                Environment={"Variables": {"TABLE_NAME": NAME}})
            print("created Lambda function")
            break
        except lam.exceptions.ResourceConflictException:
            lam.update_function_code(FunctionName=NAME, ZipFile=code)
            print("Lambda exists; code updated")
            break
        except lam.exceptions.InvalidParameterValueException:
            time.sleep(5)
    lam.get_waiter("function_active_v2").wait(FunctionName=NAME)

    try:
        url = lam.create_function_url_config(FunctionName=NAME, AuthType="NONE")["FunctionUrl"]
        lam.add_permission(FunctionName=NAME, StatementId="public-url", Action="lambda:InvokeFunctionUrl",
                           Principal="*", FunctionUrlAuthType="NONE")
    except lam.exceptions.ResourceConflictException:
        url = lam.get_function_url_config(FunctionName=NAME)["FunctionUrl"]
    config.AWS_CONFIG.write_text(json.dumps({"region": region, "function_url": url, "table": NAME}, indent=2))
    print(f"endpoint: {url}\nsaved to {config.AWS_CONFIG}")


def update(region):
    _, _, lam, _ = clients(region)
    lam.update_function_code(FunctionName=NAME, ZipFile=bundle())
    lam.get_waiter("function_updated_v2").wait(FunctionName=NAME)
    print("Lambda code updated")


def post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    t = time.perf_counter()
    with urllib.request.urlopen(req, timeout=10) as resp:
        out = json.loads(resp.read())
    return out, (time.perf_counter() - t) * 1000


def sample(i=0):
    return {"msg_id": f"test-{i}", "kind": "reading", "source": "aws_setup", "detect": True,
            "readings": [{"reading_id": f"TEST-{i:05d}", "patient": "P99", "seq": i,
                          "hr": 150, "spo2": 86, "temp": 37.0, "sbp": 120, "rr": 16}]}


def endpoint():
    url = config.load_aws_endpoint()
    if not url:
        sys.exit("not deployed - run: python aws_setup.py --deploy")
    return url


def test():
    out, ms = post(endpoint(), sample())
    print(f"round trip {ms:.1f} ms")
    print(json.dumps(out, indent=2))


def ping(n):
    url = endpoint()
    rtts = []
    for i in range(n):
        _, ms = post(url, sample(i))
        rtts.append(ms)
        print(f"  {i + 1:3d}: {ms:7.1f} ms")
    print(f"min {min(rtts):.1f}  median {statistics.median(rtts):.1f}  max {max(rtts):.1f} ms "
          f"(first request includes TLS + Lambda cold start)")


def status(region):
    ddb, _, lam, _ = clients(region)
    try:
        t = ddb.describe_table(TableName=NAME)["Table"]
        print(f"table   {NAME}: {t['TableStatus']}, ~{t.get('ItemCount', 0)} items (updated every ~6 h)")
    except ddb.exceptions.ResourceNotFoundException:
        print("table   missing")
    try:
        f = lam.get_function(FunctionName=NAME)["Configuration"]
        print(f"lambda  {NAME}: {f['State']}, {f['Runtime']}, last modified {f['LastModified']}")
        print(f"url     {lam.get_function_url_config(FunctionName=NAME)['FunctionUrl']}")
    except lam.exceptions.ResourceNotFoundException:
        print("lambda  missing")


def teardown(region):
    ddb, iam, lam, _ = clients(region)
    for step, fn in [
        ("function url", lambda: lam.delete_function_url_config(FunctionName=NAME)),
        ("lambda", lambda: lam.delete_function(FunctionName=NAME)),
        ("table", lambda: ddb.delete_table(TableName=NAME)),
        ("role policy", lambda: iam.delete_role_policy(RoleName=ROLE, PolicyName="dynamodb-write")),
        ("role attachment", lambda: iam.detach_role_policy(
            RoleName=ROLE, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")),
        ("role", lambda: iam.delete_role(RoleName=ROLE)),
    ]:
        try:
            fn()
            print(f"deleted {step}")
        except Exception as exc:
            print(f"skip {step}: {type(exc).__name__}")
    config.AWS_CONFIG.unlink(missing_ok=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--deploy", action="store_true")
    mode.add_argument("--update", action="store_true")
    mode.add_argument("--test", action="store_true")
    mode.add_argument("--ping", type=int, metavar="N")
    mode.add_argument("--status", action="store_true")
    mode.add_argument("--teardown", action="store_true")
    ap.add_argument("--region", default=None, help="default: aws_config.json, else ap-south-1")
    args = ap.parse_args()
    region = args.region
    if region is None and config.AWS_CONFIG.exists():
        region = json.loads(config.AWS_CONFIG.read_text()).get("region")
    region = region or "ap-south-1"

    if args.deploy:
        deploy(region)
    elif args.update:
        update(region)
    elif args.test:
        test()
    elif args.ping:
        ping(args.ping)
    elif args.status:
        status(region)
    else:
        teardown(region)


if __name__ == "__main__":
    main()
