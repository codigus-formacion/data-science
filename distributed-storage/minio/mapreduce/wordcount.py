import io
import boto3
from MapReduce import MapReduce  # our library

mr = MapReduce()


def mapper(line):
    # map: emit (word, 1) for every word in the line
    for word in line.lower().split():
        mr.emit_intermediate(word, 1)


def reducer(word, ones):
    # reduce: add up the occurrences of each word
    mr.emit((word, sum(ones)))


# Same code as for Amazon S3: only the endpoint changes
s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="minioadmin",
                  aws_secret_access_key="minioadmin")

# 1. Open the object as a stream: it is read in chunks, never downloaded whole
body = s3.get_object(Bucket="datos", Key="data.txt")["Body"]
lines = io.TextIOWrapper(body, encoding="utf-8")

# 2. Process it with MapReduce: map per line, reduce per word
mr.execute(lines, mapper, reducer)

# 3. Store the result as a new object, without a local file
text = "".join(f"{word}\t{count}\n" for word, count in mr.results)
s3.put_object(Bucket="datos", Key="resultados/conteo.txt", Body=text.encode("utf-8"))
