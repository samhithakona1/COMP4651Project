# Origin: original
import pytest

from src.common.taxonomy import GENAI_FRAMEWORKS, LEGACY_STACKS, MODERN_SYSTEMS, extract_skills, load_taxonomy


def has(text, skill):
    return skill in extract_skills(text)


@pytest.mark.parametrize("text,skill", [
    ("We run everything on k8s", "Kubernetes"),
    ("Kubernetes, Terraform, AWS", "Kubernetes"),
    ("KUBERNETES experience", "Kubernetes"),           # case-insensitive
    ("Our backend is written in Golang", "Go"),
    ("Stack: Python, Go, Postgres", "Go"),
    ("Go/Rust backend", "Go"),
    ("Go developer wanted", "Go"),
    ("Rust, TypeScript", "Rust"),
    ("we love rust", "Rust"),
    ("Ruby on Rails monolith", "Ruby on Rails"),
    ("Rails and Postgres", "Ruby on Rails"),
    ("Java/Spring Boot services", "Java Spring"),
    ("Spring Boot microservices", "Java Spring"),
    ("PHP 8 and Laravel", "PHP"),
    ("Apache Spark pipelines", "Apache Spark"),
    ("PySpark jobs", "Apache Spark"),
    ("Kafka, Spark, Flink", "Apache Spark"),
    ("Swift/SwiftUI iOS app", "Swift"),
    ("iOS (Swift) engineer", "Swift"),
    ("Ruby, Chef and Puppet", "Chef"),
    ("Ember.js frontend", "Ember.js"),
    ("PyTorch, Ray Serve", "Ray"),
    ("Triton Inference Server", "Triton"),
    ("CUDA/Triton kernels", "Triton"),
    ("Julia language", "Julia"),
    ("Python, R, SQL", "R"),
    ("vLLM serving", "vLLM"),
    ("LangChain agents", "LangChain"),
    ("LlamaIndex RAG", "LlamaIndex"),
    ("Hugging Face transformers", "Hugging Face"),
    ("HuggingFace", "Hugging Face"),
    ("Next.js + Tailwind", "Next.js"),
    ("Next.js + Tailwind", "Tailwind"),
    ("Node.js backend", "Node.js"),
    ("React and Redux", "React"),
    ("C++ and CUDA", "C++"),
    ("C# / .NET", "C#"),
    ("C# / .NET", ".NET"),
    ("dbt + Snowflake", "dbt"),
    ("ClickHouse analytics", "ClickHouse"),
    ("Zig or Rust", "Zig"),
    ("Elixir/Phoenix", "Elixir"),
    ("Kotlin multiplatform", "Kotlin"),
    ("OpenTelemetry, Prometheus, Grafana", "OpenTelemetry"),
    ("Helm charts", "Helm"),
    ("JAX and TPUs", "JAX"),
    ("ONNX runtime", "ONNX"),
    ("MLflow tracking", "MLflow"),
    ("TensorRT-LLM", "TensorRT"),
])
def test_positive(text, skill):
    assert has(text, skill), (text, extract_skills(text))


@pytest.mark.parametrize("text,skill", [
    ("Earn the trust of our customers", "Rust"),
    ("Nobody likes being frustrated", "Rust"),
    ("We go the extra mile for users", "Go"),
    ("Ready to go? Apply now", "Go"),
    ("Let's go to the moon", "Go"),
    ("Things that spark joy", "Apache Spark"),
    ("Our head chef cooks lunch", "Chef"),
    ("Embers of the old fire", "Ember.js"),
    ("We move with swift execution", "Swift"),
    ("Ray, our CTO, will interview you", "Ray"),
    ("Julia, our recruiter, says hi", "Julia"),
    ("Triton Digital is an audio company", "Triton"),
    ("JavaScript developer", "Java"),
    ("react quickly to incidents", "React"),
    ("Spring 2026 internship", "Java Spring"),
    ("guard rails for safety", "Ruby on Rails"),
    ("graph nodes and edges", "Node.js"),
    ("we are a remote company", "R"),
])
def test_false_positive(text, skill):
    assert not has(text, skill), (text, extract_skills(text))


def test_taxonomy_size_and_required_skills():
    names = {s.name for s in load_taxonomy()}
    assert len(names) >= 150
    required = {*GENAI_FRAMEWORKS, *MODERN_SYSTEMS, *LEGACY_STACKS, "Next.js", "Tailwind", "Node.js", "React",
                "CUDA", "JAX", "Ray", "ONNX", "MLflow", "TensorRT", "Kafka", "Flink", "Snowflake", "ClickHouse",
                "dbt", "Terraform", "Prometheus", "Grafana", "Helm", "OpenTelemetry", "Elixir", "Zig", "Swift",
                "Kotlin"}
    assert required <= names, required - names


def test_genai_frameworks_flagged_emerging():
    emerging = {s.name for s in load_taxonomy() if s.emerging_ai}
    assert set(GENAI_FRAMEWORKS) <= emerging


def test_distinct_output():
    out = extract_skills("Kubernetes k8s kube KUBERNETES")
    assert out.count("Kubernetes") == 1


def test_spark_builtin_matches_python(spark):
    """Java regex (rlike) and Python re give identical skills for the same texts."""
    from pyspark.sql import functions as F

    from src.spark.skills import skills_builtin
    texts = ["Go/Rust backend on k8s", "spark joy and trust", "C++ and C# / .NET", "Python, R, SQL",
             "Swift/SwiftUI, Kotlin", "Triton Inference Server, vLLM, Ray Serve", "Java/Spring Boot, PHP"]
    df = spark.createDataFrame([(t,) for t in texts], "t string").withColumn("s", skills_builtin(F.col("t")))
    for row in df.collect():
        assert row["s"] == extract_skills(row["t"]), row["t"]


def test_spark_udf_mode_matches_builtin(spark):
    from pyspark.sql import functions as F

    from src.spark.skills import skills_builtin, skills_udf
    texts = ["Go/Rust backend on k8s", "LangChain + LlamaIndex", "nothing here"]
    df = spark.createDataFrame([(t,) for t in texts], "t string")
    df = df.withColumn("a", skills_builtin(F.col("t"))).withColumn("b", skills_udf(spark, F.col("t")))
    for row in df.collect():
        assert row["a"] == row["b"]
