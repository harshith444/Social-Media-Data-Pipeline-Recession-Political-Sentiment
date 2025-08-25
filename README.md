# Social-Media-Data-Pipeline-Recession-Political-Sentiment

This project builds an **end-to-end ETL pipeline and analysis framework** to study how people discuss **economic recessions** and **political events** across major social media platforms.

Over **500K+ comments** were collected from Reddit, 4chan, YouTube, and r/politics in just one month, processed with NLP techniques, and visualized through an interactive dashboard to surface insights on sentiment and hate speech.

---

## 📊 Project Overview

### ETL & Data Collection
- Built an automated **ETL pipeline** that extracted 500K+ comments from Reddit, 4chan, and YouTube using official APIs.  
- Transformed raw text by cleaning, handling noise (links, emojis, HTML), detecting languages, and normalizing for consistency.  
- Loaded data into **PostgreSQL**, applying **database normalization** to reduce redundancy and ensure efficient querying.  
- Orchestrated jobs with **Apache Airflow**, enabling 24/7 automated collection and scalability.  

### Data Analysis
- Applied **NLP (NLTK, VADER)** for sentiment/emotion classification (happy, sad, angry, sorrow).  
- Integrated the **ModerateHateSpeech API** to detect toxic language with confidence thresholds.  
- **Key Findings:**  
  - Recession discussions → 55% positive, 25% negative.  
  - 4chan → 62% negative sentiment, highest among platforms.  
  - YouTube → 37.5% positive sentiment, more optimistic than 4chan.  
  - r/politics → 116K+ comments analyzed, ~90% non-toxic, highlighting strong moderation.  

### Visualization
- Developed an **interactive dashboard (Flask + PowerBI)** with filters by platform and timeframe.  
- Helped researchers compare sentiment/hate patterns across platforms, surfacing contrasts such as **4chan’s negativity vs YouTube’s optimism**.  

---

## 🛠️ Tech Stack
- **Pipeline & Orchestration**: Python, Apache Airflow  
- **Database**: PostgreSQL (**normalized schema**)  
- **ETL & Data Prep**: Pandas, Regex cleaning, Language detection  
- **NLP & Analysis**: NLTK, VADER, ModerateHateSpeech API  
- **Visualization**: Flask, PowerBI, Matplotlib  

---

## 📂 Repository Structure

```text
├── Reddit.py                 # Reddit data collection
├── Youtube_final.py          # YouTube data collection
├── chan4.py                  # 4chan data collection
├── Reddit_4chan_Analysis.py  # Combined ETL pipeline
├── Youtube_Analysis.py       # NLP + hate speech analysis
├── app.py                    # Flask web dashboard
├── index.html                # Dashboard front-end
├── subreddits.csv            # Subreddit list
├── Youtube_key.csv           # API keys (ignored in repo)
├── config*.py                # Config files (ignored in repo)
└── Proj2.ipynb               # Exploratory analysis notebook🚀 Getting Started

## Prerequisites
	•	Python 3.8+
	•	PostgreSQL (with normalized schema)
	•	Apache Airflow
	•	PowerBI Desktop 

## Installation

git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
pip install -r requirements.txt

##Run Pipeline

# Trigger via Airflow DAG
airflow dags trigger social_media_pipeline

# Or run directly
python Reddit.py
python Youtube_final.py
python chan4.py

⸻

📈 ##Sample Insights
	•	Recession-related discussions were 55% positive, despite economic uncertainty.
	•	4chan hosted the most negative tone (62% negative) vs. YouTube’s higher positivity.
	•	Political discourse in r/politics showed 90% non-toxic comments, reflecting the impact of strong moderation.

⸻

📚 ##Research Context
This project supported academic research at Binghamton University on economic discourse and political communication. The framework’s ETL pipeline and normalized database design ensure scalability for future analysis of online extremism and public sentiment.
 
⸻

🔮 ## Future Work
	•	Expand ETL to additional platforms (Twitter/X, TikTok).
	•	Train ML models for more advanced hate speech detection.
	•	Deploy dashboard online for public access.




