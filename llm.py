from tokenizers import Tokenizer, models, trainers, pre_tokenizers

words = "kavithai pidikkum enakku paattu romba pudikkum"

tokenizer = Tokenizer(models.BPE())
tokenizer.pre_tokenizer = pre_tokenizers.Whitespace()

tokenizer.train_from_iterator(
    [words],
    trainer=trainers.BpeTrainer(vocab_size=100, min_frequency=1)
)

encoded = tokenizer.encode(words)

print("Tokens:", encoded.tokens)
print("Token IDs:", encoded.ids)




# LLM-nu solradhu mostly Transformer architecture-based models.

# Naama tiny version pannanum-nu sollirukeenga, so GPT-like architecture oda simple version pothum.

# Main parts:

# Tokenizer

# Embedding layer

# Transformer blocks (attention, feedforward)

# 🔡 Text Input → 📦 Tokenization → 🔢 Token IDs → 📈 Embedding → 🧠 Transformer → 📤 Output Head → 🔡 Predicted Tokens
# Output head (logits to tokens)
