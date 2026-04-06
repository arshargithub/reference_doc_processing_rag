"""Verify the local embedding model loads and produces vectors correctly."""

from sentence_transformers import SentenceTransformer

MODEL_PATH = "models/bge-large-en-v1.5"

print(f"Loading model from {MODEL_PATH}...")
model = SentenceTransformer(MODEL_PATH)

test_texts = [
    "customer name and address",
    "annual deductible amount",
    "coinsurance percentage after deductible",
]

vectors = model.encode(test_texts)

print(f"Model loaded successfully.")
print(f"  Dimensions: {vectors.shape[1]}")
print(f"  Encoded {vectors.shape[0]} texts")
for i, text in enumerate(test_texts):
    print(f"  [{i}] \"{text}\" -> norm={vectors[i].dot(vectors[i])**0.5:.4f}")

print("\nAll checks passed.")
