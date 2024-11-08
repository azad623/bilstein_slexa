from transformers import MarianMTModel, MarianTokenizer


class ModelLoader:
    _model = None
    _tokenizer = None

    @classmethod
    def load_translation_model(cls):
        """Load the translation model and tokenizer once, if not already loaded."""
        if cls._model is None or cls._tokenizer is None:
            cls._model = MarianMTModel.from_pretrained("Helsinki-NLP/opus-mt-en-de")
            cls._tokenizer = MarianTokenizer.from_pretrained(
                "Helsinki-NLP/opus-mt-en-de"
            )
        return cls._model, cls._tokenizer
