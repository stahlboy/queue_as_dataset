"""Generate 1000 diverse Wikipedia article URLs."""

import csv

# Categories of topics to ensure diversity
topics = {
    "Science": [
        "Physics", "Chemistry", "Biology", "Astronomy", "Geology", "Meteorology",
        "Oceanography", "Paleontology", "Quantum_mechanics", "Relativity",
        "Thermodynamics", "Electromagnetism", "Optics", "Acoustics", "Particle_physics",
        "Nuclear_physics", "Atomic_physics", "Molecular_physics", "Astrophysics",
        "Cosmology", "Organic_chemistry", "Inorganic_chemistry", "Biochemistry",
        "Physical_chemistry", "Analytical_chemistry", "Materials_science", "Nanotechnology",
        "Biotechnology", "Genetics", "Ecology", "Evolution", "Microbiology", "Botany",
        "Zoology", "Neuroscience", "Immunology", "Virology", "Cell_biology", "Molecular_biology"
    ],
    "Mathematics": [
        "Algebra", "Geometry", "Calculus", "Statistics", "Topology", "Number_theory",
        "Differential_equations", "Linear_algebra", "Abstract_algebra", "Mathematical_analysis",
        "Probability_theory", "Set_theory", "Graph_theory", "Game_theory", "Combinatorics",
        "Numerical_analysis", "Complex_analysis", "Real_analysis", "Functional_analysis",
        "Logic", "Mathematical_logic", "Category_theory", "Measure_theory"
    ],
    "Technology": [
        "Artificial_intelligence", "Machine_learning", "Computer_science", "Programming_language",
        "Algorithm", "Data_structure", "Operating_system", "Database", "Network_protocol",
        "Cryptography", "Computer_graphics", "Software_engineering", "Web_development",
        "Mobile_app", "Cloud_computing", "Quantum_computing", "Robotics", "Internet_of_things",
        "Blockchain", "Virtual_reality", "Augmented_reality", "3D_printing", "Semiconductor"
    ],
    "Medicine": [
        "Anatomy", "Physiology", "Pathology", "Pharmacology", "Surgery", "Medicine",
        "Cardiology", "Neurology", "Oncology", "Pediatrics", "Psychiatry", "Radiology",
        "Immunology", "Epidemiology", "Public_health", "Nursing", "Dentistry",
        "Veterinary_medicine", "Medical_imaging", "Clinical_trial", "Drug", "Vaccine"
    ],
    "History": [
        "Ancient_history", "Medieval_history", "Modern_history", "World_War_I", "World_War_II",
        "American_Revolution", "French_Revolution", "Industrial_Revolution", "Renaissance",
        "Enlightenment", "Cold_War", "Roman_Empire", "Byzantine_Empire", "Ottoman_Empire",
        "British_Empire", "Mongol_Empire", "Ancient_Egypt", "Ancient_Greece", "Ancient_Rome",
        "Viking_Age", "Crusades", "Age_of_Discovery", "Colonialism"
    ],
    "Geography": [
        "Africa", "Asia", "Europe", "North_America", "South_America", "Australia", "Antarctica",
        "Pacific_Ocean", "Atlantic_Ocean", "Indian_Ocean", "Arctic_Ocean", "Mediterranean_Sea",
        "Sahara", "Amazon_rainforest", "Himalayas", "Rocky_Mountains", "Andes", "Alps",
        "Great_Barrier_Reef", "Nile", "Amazon_River", "Yangtze", "Mississippi_River"
    ],
    "Arts": [
        "Painting", "Sculpture", "Music", "Dance", "Theater", "Film", "Photography",
        "Architecture", "Literature", "Poetry", "Novel", "Drama", "Opera", "Ballet",
        "Jazz", "Classical_music", "Rock_music", "Hip_hop", "Pop_music", "Folk_music",
        "Renaissance_art", "Baroque", "Impressionism", "Modernism", "Postmodernism"
    ],
    "Philosophy": [
        "Ethics", "Metaphysics", "Epistemology", "Logic", "Aesthetics", "Political_philosophy",
        "Philosophy_of_mind", "Philosophy_of_science", "Existentialism", "Phenomenology",
        "Pragmatism", "Rationalism", "Empiricism", "Idealism", "Materialism", "Dualism",
        "Utilitarianism", "Deontology", "Virtue_ethics", "Stoicism", "Platonism"
    ],
    "Social_Sciences": [
        "Sociology", "Psychology", "Anthropology", "Economics", "Political_science",
        "Linguistics", "Archaeology", "Demography", "Human_geography", "Social_psychology",
        "Cognitive_psychology", "Developmental_psychology", "Behavioral_economics",
        "Macroeconomics", "Microeconomics", "International_relations", "Comparative_politics",
        "Cultural_anthropology", "Linguistic_anthropology"
    ],
    "Sports": [
        "Football", "Basketball", "Baseball", "Tennis", "Golf", "Cricket", "Rugby",
        "Ice_hockey", "Volleyball", "Swimming", "Athletics", "Gymnastics", "Boxing",
        "Martial_arts", "Cycling", "Skiing", "Skateboarding", "Surfing", "Climbing"
    ],
    "Nature": [
        "Biodiversity", "Ecosystem", "Rainforest", "Desert", "Tundra", "Coral_reef",
        "Wetland", "Prairie", "Savanna", "Taiga", "Climate_change", "Conservation_biology",
        "Endangered_species", "Habitat", "Food_chain", "Photosynthesis", "Water_cycle",
        "Carbon_cycle", "Nitrogen_cycle", "Natural_selection"
    ],
    "Culture": [
        "Religion", "Christianity", "Islam", "Buddhism", "Hinduism", "Judaism",
        "Mythology", "Greek_mythology", "Norse_mythology", "Egyptian_mythology",
        "Folklore", "Festival", "Tradition", "Custom", "Ritual", "Ceremony",
        "Language", "Writing_system", "Alphabet", "Symbol", "Sign_language"
    ],
    "Countries": [
        "United_States", "China", "India", "Russia", "Japan", "Germany", "United_Kingdom",
        "France", "Italy", "Spain", "Canada", "Australia", "Brazil", "Mexico", "Argentina",
        "South_Korea", "Indonesia", "Turkey", "Saudi_Arabia", "Egypt", "South_Africa",
        "Nigeria", "Kenya", "Ethiopia", "Poland", "Ukraine", "Sweden", "Norway", "Denmark"
    ],
    "Cities": [
        "New_York_City", "London", "Paris", "Tokyo", "Beijing", "Shanghai", "Hong_Kong",
        "Singapore", "Dubai", "Mumbai", "Delhi", "Seoul", "Moscow", "Istanbul", "Cairo",
        "Los_Angeles", "Chicago", "San_Francisco", "Boston", "Washington,_D.C.", "Toronto",
        "Vancouver", "Sydney", "Melbourne", "Berlin", "Rome", "Madrid", "Barcelona", "Amsterdam"
    ],
    "Inventions": [
        "Wheel", "Printing_press", "Steam_engine", "Electricity", "Telephone", "Radio",
        "Television", "Computer", "Internet", "Airplane", "Automobile", "Bicycle",
        "Camera", "Microscope", "Telescope", "Compass", "Clock", "Battery", "Light_bulb",
        "Refrigerator", "Air_conditioning", "Elevator", "Typewriter", "Calculator"
    ],
    "Food": [
        "Bread", "Rice", "Wheat", "Corn", "Potato", "Tomato", "Apple", "Banana",
        "Orange", "Grape", "Coffee", "Tea", "Chocolate", "Sugar", "Salt", "Pepper",
        "Cheese", "Milk", "Butter", "Egg", "Meat", "Fish", "Vegetable", "Fruit"
    ],
    "Energy": [
        "Solar_energy", "Wind_power", "Hydroelectricity", "Nuclear_power", "Fossil_fuel",
        "Coal", "Petroleum", "Natural_gas", "Renewable_energy", "Biofuel", "Geothermal_energy",
        "Tidal_power", "Wave_power", "Energy_storage", "Battery", "Fuel_cell"
    ],
    "Transportation": [
        "Railway", "Highway", "Bridge", "Tunnel", "Airport", "Seaport", "Ship", "Boat",
        "Submarine", "Helicopter", "Rocket", "Spacecraft", "Satellite", "Space_station",
        "Metro", "Tram", "Bus", "Taxi", "Truck", "Motorcycle", "Scooter"
    ],
    "Materials": [
        "Steel", "Iron", "Copper", "Aluminum", "Gold", "Silver", "Plastic", "Rubber",
        "Glass", "Ceramic", "Wood", "Paper", "Concrete", "Cement", "Diamond", "Graphite",
        "Silicon", "Carbon_fiber", "Titanium", "Brass", "Bronze"
    ],
    "Biology_Topics": [
        "DNA", "RNA", "Protein", "Enzyme", "Chromosome", "Gene", "Mutation", "Genome",
        "Cell", "Tissue", "Organ", "Organism", "Species", "Population", "Community",
        "Metabolism", "Respiration", "Digestion", "Circulation", "Nervous_system",
        "Endocrine_system", "Immune_system", "Reproductive_system"
    ]
}

# Generate URLs
urls = []
for category, items in topics.items():
    for item in items:
        urls.append(f"https://en.wikipedia.org/wiki/{item}")

# Add more general topics to reach 1000
additional_topics = [
    "Education", "University", "School", "Teacher", "Student", "Research", "Science",
    "Knowledge", "Information", "Communication", "Media", "Journalism", "Newspaper",
    "Magazine", "Book", "Library", "Museum", "Art_gallery", "Concert_hall", "Stadium",
    "Park", "Garden", "Forest", "Mountain", "River", "Lake", "Sea", "Island", "Peninsula",
    "Continent", "Country", "State", "Province", "City", "Town", "Village", "Population",
    "Demographics", "Migration", "Immigration", "Emigration", "Urbanization", "Globalization",
    "Trade", "Commerce", "Industry", "Manufacturing", "Agriculture", "Mining", "Fishing",
    "Forestry", "Tourism", "Hospitality", "Service_industry", "Retail", "Wholesale",
    "Finance", "Banking", "Insurance", "Investment", "Stock_market", "Currency", "Money",
    "Tax", "Budget", "Debt", "Credit", "Loan", "Mortgage", "Interest_rate", "Inflation",
    "Recession", "Depression", "Growth", "Development", "Poverty", "Wealth", "Income",
    "Employment", "Unemployment", "Labor", "Union", "Strike", "Wage", "Salary", "Pension",
    "Retirement", "Social_security", "Welfare", "Healthcare", "Hospital", "Clinic", "Pharmacy",
    "Emergency_room", "Intensive_care_unit", "Ambulance", "Paramedic", "Nurse", "Physician",
    "Surgeon", "Specialist", "General_practitioner", "Pediatrician", "Obstetrician",
    "Gynecologist", "Cardiologist", "Neurologist", "Oncologist", "Psychiatrist", "Psychologist",
    "Therapist", "Counselor", "Social_worker", "Pharmacist", "Dentist", "Orthodontist",
    "Optometrist", "Veterinarian", "Medical_device", "Medical_test", "Diagnosis", "Treatment",
    "Therapy", "Medication", "Prescription", "Over-the-counter_drug", "Antibiotic", "Vaccine",
    "Anesthesia", "Surgery", "Transplantation", "Blood_transfusion", "Dialysis", "Chemotherapy",
    "Radiation_therapy", "Physical_therapy", "Occupational_therapy", "Speech_therapy",
    "Mental_health", "Disability", "Chronic_disease", "Infectious_disease", "Pandemic",
    "Epidemic", "Outbreak", "Quarantine", "Isolation", "Hygiene", "Sanitation", "Water_quality",
    "Air_quality", "Pollution", "Waste", "Recycling", "Sustainability", "Green_energy",
    "Climate", "Weather", "Temperature", "Precipitation", "Wind", "Storm", "Hurricane",
    "Tornado", "Earthquake", "Volcano", "Tsunami", "Flood", "Drought", "Wildfire", "Avalanche",
    "Landslide", "Erosion", "Deforestation", "Desertification", "Ocean_acidification",
    "Ozone_depletion", "Greenhouse_gas", "Carbon_dioxide", "Methane", "Nitrous_oxide",
    "Renewable_resource", "Non-renewable_resource", "Fossil_fuel_phase-out", "Energy_transition",
    "Electric_vehicle", "Hybrid_vehicle", "Public_transport", "Sustainable_transport",
    "Urban_planning", "Architecture", "Construction", "Building", "House", "Apartment",
    "Skyscraper", "Tower", "Castle", "Palace", "Temple", "Church", "Mosque", "Synagogue",
    "Cathedral", "Monastery", "Pagoda", "Pyramid", "Sphinx", "Colosseum", "Parthenon",
    "Great_Wall_of_China", "Taj_Mahal", "Eiffel_Tower", "Statue_of_Liberty", "Big_Ben",
    "Tower_of_London", "Buckingham_Palace", "White_House", "Capitol", "Pentagon",
    "United_Nations", "World_Bank", "International_Monetary_Fund", "World_Trade_Organization",
    "NATO", "European_Union", "African_Union", "ASEAN", "OPEC", "G7", "G20", "BRICS",
    "Democracy", "Republic", "Monarchy", "Dictatorship", "Totalitarianism", "Authoritarianism",
    "Fascism", "Communism", "Socialism", "Capitalism", "Liberalism", "Conservatism",
    "Nationalism", "Internationalism", "Imperialism", "Colonialism", "Decolonization",
    "Independence", "Sovereignty", "Self-determination", "Human_rights", "Civil_rights",
    "Freedom_of_speech", "Freedom_of_religion", "Freedom_of_assembly", "Right_to_vote",
    "Equality", "Justice", "Law", "Constitution", "Legislation", "Court", "Judge", "Jury",
    "Trial", "Evidence", "Testimony", "Verdict", "Sentence", "Prison", "Punishment",
    "Rehabilitation", "Parole", "Probation", "Crime", "Criminal", "Victim", "Police",
    "Detective", "Investigation", "Forensics", "DNA_profiling", "Fingerprint", "Ballistics",
    "Cybercrime", "Fraud", "Theft", "Robbery", "Burglary", "Assault", "Murder", "Homicide",
    "Terrorism", "War", "Peace", "Conflict", "Negotiation", "Treaty", "Alliance", "Diplomacy",
    "Ambassador", "Embassy", "Consul", "Foreign_policy", "National_security", "Defense",
    "Military", "Army", "Navy", "Air_force", "Marines", "Coast_guard", "Soldier", "Sailor",
    "Pilot", "Officer", "General", "Admiral", "Weapon", "Gun", "Rifle", "Pistol", "Machine_gun",
    "Artillery", "Tank", "Fighter_aircraft", "Bomber", "Missile", "Nuclear_weapon", "Warship",
    "Aircraft_carrier", "Submarine", "Destroyer", "Frigate", "Corvette", "Patrol_boat",
    "Strategy", "Tactics", "Battle", "Campaign", "Siege", "Blockade", "Invasion", "Occupation",
    "Resistance", "Guerrilla_warfare", "Civil_war", "Revolution", "Coup", "Insurgency",
    "Rebellion", "Uprising", "Protest", "Demonstration", "Strike", "Boycott", "Sanctions",
    "Embargo", "Blockade", "Humanitarian_aid", "Relief_effort", "Refugee", "Asylum",
    "Displacement", "Migration_crisis", "Border", "Customs", "Passport", "Visa", "Immigration_policy",
    "Citizenship", "Naturalization", "Deportation", "Statelessness", "International_law",
    "War_crime", "Genocide", "Ethnic_cleansing", "Crimes_against_humanity", "Geneva_Conventions",
    "Red_Cross", "Humanitarian_intervention", "Peacekeeping", "Peace_treaty", "Armistice",
    "Ceasefire", "Reconciliation", "Transitional_justice", "Truth_commission", "Reparations",
    "Memory", "Commemoration", "Memorial", "Monument", "Heritage", "World_Heritage_Site",
    "UNESCO", "Cultural_heritage", "Intangible_cultural_heritage", "Archaeology", "Excavation",
    "Artifact", "Museum", "Exhibition", "Collection", "Conservation", "Restoration", "Preservation",
    "Archive", "Manuscript", "Document", "Record", "Chronicle", "History", "Historiography",
    "Primary_source", "Secondary_source", "Oral_history", "Biography", "Autobiography", "Memoir",
    "Diary", "Letter", "Correspondence", "Periodical", "Journal", "Academic_journal", "Peer_review",
    "Scientific_method", "Hypothesis", "Theory", "Experiment", "Observation", "Data", "Statistics",
    "Analysis", "Interpretation", "Conclusion", "Publication", "Citation", "Reference",
    "Bibliography", "Index", "Abstract", "Introduction", "Methodology", "Results", "Discussion",
    "Literature_review", "Meta-analysis", "Systematic_review", "Case_study", "Survey", "Interview",
    "Focus_group", "Ethnography", "Participant_observation", "Field_research", "Laboratory",
    "Equipment", "Instrument", "Measurement", "Calibration", "Accuracy", "Precision", "Error",
    "Uncertainty", "Reproducibility", "Replication", "Validation", "Verification", "Quality_control",
    "Standard", "Protocol", "Procedure", "Guidelines", "Best_practice", "Regulation", "Compliance",
    "Certification", "Accreditation", "License", "Patent", "Copyright", "Trademark", "Intellectual_property",
    "Trade_secret", "Confidentiality", "Non-disclosure_agreement", "Contract", "Agreement",
    "Partnership", "Collaboration", "Cooperation", "Coordination", "Network", "Alliance",
    "Association", "Organization", "Institution", "Foundation", "Trust", "Charity", "Non-profit",
    "NGO", "Volunteer", "Donation", "Fundraising", "Grant", "Scholarship", "Fellowship",
    "Award", "Prize", "Recognition", "Achievement", "Success", "Excellence", "Innovation",
    "Creativity", "Imagination", "Inspiration", "Motivation", "Leadership", "Management",
    "Administration", "Governance", "Policy", "Planning", "Strategy", "Decision-making",
    "Problem-solving", "Critical_thinking", "Analytical_thinking", "Logical_thinking",
    "Creative_thinking", "Systems_thinking", "Design_thinking", "Lateral_thinking"
]

for topic in additional_topics:
    if len(urls) >= 1000:
        break
    urls.append(f"https://en.wikipedia.org/wiki/{topic}")

# Take exactly 1000
urls = urls[:1000]

# Write to CSV
with open("wikipedia_1000.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["url"])
    for url in urls:
        writer.writerow([url])

print(f"Generated {len(urls)} Wikipedia URLs")
print("Saved to wikipedia_1000.csv")
