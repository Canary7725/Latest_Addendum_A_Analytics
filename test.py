import nltk  # Provides tools for processing human language.
import numpy as np  # Provides numerical arrays used in the similarity calculation.

from nltk.tokenize import sent_tokenize, word_tokenize  # Import sentence and word tokenizers.
from nltk.corpus import stopwords  # Import access to common-word lists, such as "the" and "and".
from nltk.stem import WordNetLemmatizer  # Import a tool that converts words to their dictionary forms.
from sklearn.feature_extraction.text import TfidfVectorizer  # Convert sentences into weighted numerical vectors.
from sklearn.metrics.pairwise import cosine_similarity  # Measure similarity between numerical vectors.


# DOWNLOAD NLTK RESOURCES — Internet access is needed on the first run.

nltk.download("punkt", quiet=True)  # Download sentence-tokenization resources; quiet=True hides routine messages.
nltk.download("punkt_tab", quiet=True)  # Download additional tokenizer resources used by newer NLTK versions.
nltk.download("stopwords", quiet=True)  # Download lists of common words for different languages.
nltk.download("wordnet", quiet=True)  # Download the dictionary resource used for lemmatization.


# PREPARE THE TEXT-PROCESSING TOOLS.

stop_words = set(stopwords.words("english"))  # Load English stopwords into a set for fast membership checks.
lemmatizer = WordNetLemmatizer()  # Create a reusable lemmatizer; its default behavior treats words as nouns.


def preprocess_sentence(sentence):  # Define a function that converts one sentence into cleaned words.
    tokens = word_tokenize(sentence.lower())  # Lowercase the sentence, then split it into words and punctuation.
    cleaned_words = []  # Create an empty list to collect the words we keep.

    for word in tokens:  # Examine each token in the sentence.
        if word.isalpha() and word not in stop_words:  # Keep only alphabetic tokens that are not stopwords.
            base_word = lemmatizer.lemmatize(word)  # Convert noun forms such as "trees" to "tree".
            cleaned_words.append(base_word)  # Add the normalized word to our list.

    return " ".join(cleaned_words)  # Combine the cleaned words into a space-separated string.


def summarize(text, num_sentences=2):
    sentences = sent_tokenize(text.strip())  
    # Remove outer whitespace, then split the original text into sentences.

    if num_sentences <= 0:  # Check whether the requested summary length is zero or negative.
        return "" 

    if len(sentences) <= num_sentences:  # Check whether the text is already short enough.
        return " ".join(sentences)  

    cleaned_sentences = [] 

    for sentence in sentences:  # Visit each original sentence in order.
        cleaned_sentences.append(preprocess_sentence(sentence))  # Clean it with NLTK and save the result.

    vectorizer = TfidfVectorizer(tokenizer=str.split, token_pattern=None)  
    # Split on spaces because NLTK already cleaned and lowercased the words.

    try:  
        vectors = vectorizer.fit_transform(cleaned_sentences)  
        # Learn vocabulary and IDF weights, then create one sparse TF-IDF vector per sentence.
    except ValueError:  # Handle an empty vocabulary, such as when every word was removed.
        return " ".join(sentences[:num_sentences])

    paragraph__average_vector = np.asarray(vectors.mean(axis=0))  
    # Average each vocabulary column and convert the result to a NumPy array.

    scores = cosine_similarity(vectors, paragraph__average_vector).ravel()
    # Compare every sentence vector with the paragraph's average vector.
    # Flatten the result from shape (sentence_count, 1) into a one-dimensional score array.

    ranked_indices = sorted(  # Create a new list of sentence positions ordered by score.
        range(len(sentences)),  # Generate sentence indices: 0, 1, 2, and so on.
        key=lambda index: scores[index],  # Use each sentence's similarity score as its sorting value.
        reverse=True  # Sort from the highest score to the lowest.
    )  # Finish ranking the sentence indices.

    selected_indices = ranked_indices[:num_sentences]  # Keep only the requested number of highest-scoring indices.
    selected_indices.sort()  # Restore their original order so the summary follows the paragraph's sequence.

    summary = " ".join(sentences[index] for index in selected_indices)  # Join the selected ORIGINAL sentences, preserving their wording.
    return summary  # Send the completed summary back to the caller.


# EXAMPLE INPUT — Parenthesized strings are joined automatically by Python.

paragraph = (  # Start a single paragraph assembled from several string literals.
    "Trees play an important role in protecting the environment. "  # First sentence; the trailing space separates it from the next.
    "Trees absorb carbon dioxide and release oxygen into the air. "  # Second sentence.
    "Forests provide shelter for many species of birds and animals. "  # Third sentence.
    "Cutting down forests destroys habitats and increases carbon dioxide. "  # Fourth sentence.
    "Protecting trees and forests helps preserve the environment."  # Fifth sentence.
)  # Finish constructing the paragraph string.

result = summarize(paragraph, num_sentences=2)  # Generate a summary containing two selected sentences.
print("Original paragraph:\n", paragraph)  # Display the original text; \n inserts a newline.
print("\nSummary:\n", result)  # Display the summary below the original text.