# Topic modelling test run

from bertopic import BERTopic
import os

model_path = os.getcwd() + "/models/"
embedding_model_path = os.getcwd() + "/all-MiniLM-L6-v2"

# Load the BERTopic model
topic_model = BERTopic.load(model_path, embedding_model=embedding_model_path)

# Example string to predict the topic for
input_text = "President"

# Predict the topic for the input text
predicted_topic, _ = topic_model.transform([input_text])

# Print the predicted topic
print("Predicted Topic:", predicted_topic[0])
predicted_topic_label = topic_model.get_topic(predicted_topic[0])#[0]

# The confidence levels/probability are stored beside the predictions
topics = list()
for label in predicted_topic_label:
    topics.append(label[0])

print("Labeles/Topics:\t{}".format(topics))

# Generating a word cloud
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Convert the list to a single string
text = ' '.join(topics)

# Generate the word cloud
wordcloud = WordCloud(width=800, height=800, background_color='white', min_font_size=10).generate(text)

# Plot the word cloud
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wordcloud)
plt.axis("off")
plt.tight_layout(pad=0)

plt.show()
