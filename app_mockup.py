import streamlit as st
import json
import os
from schema.models import DailyPayload

# Configurare pagină tip "Dark Premium"
st.set_page_config(layout="wide", page_title="HistoryDaily Global", page_icon="🌍")

# CSS Custom pentru UI/UX de top
st.markdown("""
    <style>
    .stApp { background-color: #0d1117; }
    .main-header { font-size: 3.5rem; font-weight: 800; color: #FFD700; line-height: 1.2; margin-bottom: 20px; }
    .event-card { 
        background-color: #161b22; 
        border-radius: 15px; 
        padding: 25px; 
        border: 1px solid #30363d; 
        margin-bottom: 20px;
        transition: transform 0.3s ease;
    }
    .event-card:hover { border-color: #FFD700; transform: translateY(-5px); }
    .side-card {
        background-color: #0d1117;
        border-radius: 12px;
        padding: 15px;
        border: 1px solid #30363d;
        display: flex;
        gap: 15px;
        align-items: center;
        margin-bottom: 10px;
    }
    .score-badge { 
        background: linear-gradient(90deg, #FFD700, #FFA500); 
        color: black; 
        padding: 4px 12px; 
        border-radius: 20px; 
        font-weight: bold; 
        font-size: 0.8rem;
    }
    .lang-label { color: #8b949e; font-size: 0.9rem; margin-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)


def load_payload():
    path = "daily_payload_ready_for_java.json"
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return DailyPayload.model_validate(json.load(f))
    return None


data = load_payload()

if not data:
    st.error("❌ Nu am găsit datele. Rulează mai întâi `main.py`!")
else:
    # --- SIDEBAR: Control Panel ---
    with st.sidebar:
        st.image("https://cdn-icons-png.flaticon.com/512/3602/3602143.png", width=100)
        st.title("Admin Panel")

        # Selector de Limbă (Cheia succesului tău global)
        lang_options = {
            "🇷🇴 Română": "ro",
            "🇺🇸 English": "en",
            "🇪🇸 Español": "es",
            "🇫🇷 Français": "fr",
            "🇩🇪 Deutsch": "de"
        }
        selected_lang_name = st.selectbox("🌍 Schimbă Limba / Select Language", list(lang_options.keys()))
        L = lang_options[selected_lang_name]  # Aceasta este cheia (ex: 'ro')

        st.divider()
        st.info(f"📅 Data Procesării: {data.date_processed}")
        st.metric("Scor Impact", f"{data.main_event.impact_score}/100")

    # --- MAIN EVENT: Hero Section ---
    main = data.main_event

    col1, col2 = st.columns([1.2, 1])

    with col1:
        # Titlu tradus dinamic
        title = main.title_translations.model_dump().get(L, main.title_translations.en)
        st.markdown(f"<h1 class='main-header'>{title}</h1>", unsafe_allow_html=True)
        st.markdown(f"### 📅 Year {main.year}")

        # Galeria Hero
        if main.gallery:
            st.image(main.gallery[0], use_container_width=True)
            if len(main.gallery) > 1:
                cols = st.columns(len(main.gallery) - 1)
                for idx, img in enumerate(main.gallery[1:]):
                    cols[idx].image(img, use_container_width=True)

    with col2:
        st.markdown("<div class='event-card'>", unsafe_allow_html=True)
        # Narațiune tradusă dinamic
        narrative = main.narrative_translations.model_dump().get(L, main.narrative_translations.en)
        st.markdown(narrative)
        st.markdown("</div>", unsafe_allow_html=True)
        st.link_button(f"🔍 Wikipedia ({selected_lang_name})", main.source_url)

    st.divider()

    # --- SECONDARY EVENTS: List Section ---
    st.subheader(f"📚 {'Alte evenimente' if L == 'ro' else 'Other events'}")

    # Grid pentru evenimente secundare
    for sec in data.secondary_events:
        sec_title = sec.title_translations.model_dump().get(L, sec.title_translations.en)

        with st.container():
            # Folosim un layout tip rând (imagine mică + text)
            c1, c2 = st.columns([1, 4])
            with c1:
                if sec.thumbnail_url:
                    st.image(sec.thumbnail_url, use_container_width=True)
                else:
                    st.image("https://via.placeholder.com/150/161b22/ffd700?text=History", use_container_width=True)

            with c2:
                st.markdown(f"""
                <div style="padding-top: 10px;">
                    <span class="score-badge">AN {sec.year}</span>
                    <h4 style="margin-top: 5px;">{sec_title}</h4>
                    <a href="{sec.source_url}" target="_blank" style="color: #FFD700; text-decoration: none; font-size: 0.8rem;">Citește mai mult →</a>
                </div>
                """, unsafe_allow_html=True)
        st.markdown("<hr style='border: 0.1px solid #30363d;'>", unsafe_allow_html=True)

    # Metadata Debug
    with st.expander("🛠️ System Metadata"):
        st.json(data.metadata)