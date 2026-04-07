# -*- coding: utf-8 -*-
"""
O'zbek ismlarining to'liq ro'yxati - erkaklar va ayollar uchun.
Bu ro'yxat Whisper va Grok AI ga O'zbek ismlarini to'g'ri tanib olishga yordam beradi.

Manba: O'zbekistonda eng keng tarqalgan ismlar
"""

# ============================================================
# ERKAK ISMLARI (Men's Names)
# ============================================================
UZBEK_MALE_NAMES = [
    # A
    "Abdulla", "Abdulloh", "Abdurahim", "Abdurahmon", "Abdurashid", "Abdusalom",
    "Abdusamad", "Abduvali", "Abduvohid", "Abror", "Abrorbek", "Ahadjon",
    "Ahmad", "Ahmadbek", "Ahmadjon", "Akbar", "Akbarbek", "Akbarjon",
    "Akmal", "Akmalbek", "Akmaljon", "Akram", "Akrambek", "Akramjon",
    "Ali", "Alibek", "Alijon", "Alisher", "Alisherbek", "Alisherjon",
    "Anvar", "Anvarbek", "Anvarjon", "Asad", "Asadbek", "Asadjon",
    "Askar", "Askarbek", "Askarjon", "Asliddin", "Asror", "Asrorbek",
    "Azam", "Azambek", "Azamjon", "Aziz", "Azizbek", "Azizjon",
    "Azimjon", "Azamat", "Azamatbek",
    
    # B
    "Bahrom", "Bahrombek", "Bahromjon", "Baxtiyor", "Baxtiyorbek", "Baxtiyorjon",
    "Baxrom", "Baxrombek", "Baxromjon", "Bekzod", "Bekzodbek", "Behzod",
    "Bilol", "Bilolbek", "Biloljon", "Bobur", "Boburbek", "Boburjon",
    "Botir", "Botirbek", "Botirjon", "Bunyod", "Bunyodbek", "Bunyodjon",
    
    # D
    "Davlat", "Davlatbek", "Davlatjon", "Davron", "Davronbek", "Davronjon",
    "Dilmurod", "Dilmurodbek", "Dilmurodjon", "Dilshod", "Dilshodbek", "Dilshodjon",
    "Doniyor", "Doniyorbek", "Doniyorjon", "Dostonbek", "Dostonjon",
    
    # E
    "Elbek", "Eldor", "Eldorbek", "Eldorjon", "Elmurod", "Elmurodbek",
    "Elyor", "Elyorbek", "Elyorjon", "Erkin", "Erkinbek", "Erkinjon",
    
    # F
    "Farrux", "Farruxbek", "Farruxjon", "Farxod", "Farxodbek", "Farxodjon",
    "Farhod", "Farhodbek", "Farhodjon", "Fazliddin", "Feruz", "Feruzbek",
    
    # G
    "G'ayrat", "G'ayratbek", "G'ayratjon", "G'olibjon",
    
    # H
    "Humoyun", "Humoyunbek", "Husanboy", "Husanbek", "Husanjon",
    
    # I
    "Ibrohim", "Ibrohimbek", "Ibrohimjon", "Ikrom", "Ikrombek", "Ikromjon",
    "Ilhom", "Ilhombek", "Ilhomjon", "Ilxom", "Ilxombek", "Ilxomjon",
    "Islom", "Islombek", "Islomjon", "Ismoil", "Ismoilbek", "Ismoiljon",
    "Izzat", "Izzatbek", "Izzatjon",
    
    # J
    "Jahongir", "Jahongirbek", "Jahongirjon", "Jalol", "Jalolbek", "Jaloljon",
    "Jamol", "Jamolbek", "Jamoljon", "Jamshid", "Jamshidbek", "Jamshidjon",
    "Jasur", "Jasurbek", "Jasurjon", "Javlon", "Javlonbek", "Javlonjon",
    "Javohir", "Javohirbek", "Javohirjon", "Jaxongir",
    
    # K
    "Kamoliddin", "Kamol", "Kamolbek", "Kamoljon", "Kamron", "Kamronbek",
    "Karim", "Karimbek", "Karimjon", "Komil", "Komilbek", "Komiljon",
    "Kozim", "Kozimbek", "Kozimjon", "Kuvonchbek", "Kuvonchjon",
    
    # L
    "Laziz", "Lazizbek", "Lazizjon", "Lochin", "Lochinbek", "Lochinjon",
    
    # M
    "Mahmud", "Mahmudbek", "Mahmudjon", "Mansur", "Mansurbek", "Mansurjon",
    "Mardon", "Mardonbek", "Mardonjon", "Miraziz", "Mirazizbek",
    "Mirjalol", "Mirjalolbek", "Mirzo", "Mirzobek", "Mirzojon",
    "Muhammadali", "Muhammad", "Muhammadbek", "Muhammadjon",
    "Muhammadrasul", "Muhammadsodiq", "MuhammadRizo", "Muhammadziyo",
    "Murod", "Murodbek", "Murodjon", "Murodali", "Murodilla",
    "Muzaffar", "Muzaffarbek", "Muzaffarjon", "Muxammad", "Muxammadbek",
    
    # N
    "Nabijon", "Nafis", "Nafisbek", "Nafisjon", "Najmiddin", "Najmiddinbek",
    "Nasriddin", "Nasriddinbek", "Nasrulloh", "Navro'z", "Navro'zbek",
    "Nizom", "Nizombek", "Nizomjon", "No'mon", "No'monbek", "No'monjon",
    "Nodirjon", "Nodirbek", "Nozim", "Nozimbek", "Nozimjon",
    "Nurali", "Nuralibek", "Nuralijon", "Nurillo", "Nurillobek",
    "Nurmuhammad", "Nurmuhammadbek",
    
    # O
    "Obid", "Obidbek", "Obidjon", "Odil", "Odilbek", "Odiljon",
    "Ogabek", "Olim", "Olimbek", "Olimjon", "Omadbek", "Omadjon",
    "Ortiq", "Ortiqbek", "Ortiqjon", "Ota", "Otabek", "Otajon",
    "Oybek", "Oybekjon", "Oqiljon",
    
    # P
    "Pahlavon", "Pulat", "Pulatbek", "Pulatjon",
    
    # Q
    "Qodir", "Qodirbek", "Qodirjon", "Qudrat", "Qudratbek", "Qudratjon",
    
    # R
    "Rahmat", "Rahmatbek", "Rahmatjon", "Rahmatulla", "Rahmatulloh",
    "Rasul", "Rasulbek", "Rasuljon", "Ravshan", "Ravshanbek", "Ravshanjon",
    "Rustam", "Rustambek", "Rustamjon", "Ruzibek", "Ruziboy",
    
    # S
    "Safarali", "Safari", "Safaribek", "Said", "Saidbek", "Saidjon",
    "Saidakbar", "Saidamir", "Salim", "Salimbek", "Salimjon",
    "Salohiddin", "Salohiddinbek", "Samandar", "Samandarbek", "Samandarjon",
    "Sanjar", "Sanjarbek", "Sanjarjon", "Sardor", "Sardorbek", "Sardorjon",
    "Sarvar", "Sarvarbek", "Sarvarjon", "Sattor", "Sattorbek", "Sattorjon",
    "Shavkat", "Shavkatbek", "Shavkatjon", "Sherali", "Sheralibek",
    "Sherbek", "Sherjon", "Sherzod", "Sherzodbek", "Sherzodjon",
    "Shodibek", "Shodijon", "Shohrux", "Shohruxbek", "Shohruxjon",
    "Shoxrux", "Shoxruxbek", "Shoxruxjon", "Shuhrat", "Shuhratbek",
    "Shukur", "Shukurbek", "Shukurjon", "Sirojiddin", "Sirojiddinbek",
    "Sobir", "Sobirbek", "Sobirjon", "Sodiq", "Sodiqbek", "Sodiqjon",
    "Suxrob", "Suxrobbek", "Suxrobjon", "Sulton", "Sultonbek", "Sultonjon",
    
    # T
    "Temur", "Temurbek", "Temurjon", "Tohir", "Tohirbek", "Tohirjon",
    "Tolibjon", "Tolib", "Tolibek", "To'lqin", "To'lqinbek", "To'lqinjon",
    "Tursunali", "Tursunbek", "Tursunboy",
    
    # U
    "Ulug'bek", "Ulugbek", "Ulug'bekjon", "Umid", "Umidbek", "Umidjon",
    "Usmon", "Usmonbek", "Usmonjon", "Uygun", "Uygunbek", "Uygunjon",
    
    # V
    "Vali", "Valibek", "Valijon", "Vohid", "Vohidbek", "Vohidjon",
    
    # X
    "Xasan", "Xasanbek", "Xasanboy", "Xolmurod", "Xolmurodbek",
    "Xojiakbar", "Xojiakbarbek", "Xurshid", "Xurshidbek", "Xurshidjon",
    
    # Y
    "Yaxyo", "Yaxyobek", "Yaxyojon", "Yigitali", "Yigitbek",
    "Yodgor", "Yodgorbek", "Yodgorjon", "Yoqub", "Yoqubbek", "Yoqubjon",
    "Yuldash", "Yuldashbek", "Yuldashboy", "Yusuf", "Yusufbek", "Yusufjon",
    
    # Z
    "Zafar", "Zafarbek", "Zafarjon", "Zarif", "Zarifbek", "Zarifjon",
    "Ziyod", "Ziyodbek", "Ziyodjon", "Zokir", "Zokirbek", "Zokirjon",
    "Zuxriddin", "Zuxriddinbek",
]

# ============================================================
# AYOL ISMLARI (Women's Names)
# ============================================================
UZBEK_FEMALE_NAMES = [
    # A
    "Adolat", "Afsona", "Aida", "Ainura", "Aziza", "Azizaxon",
    "Anora", "Asila", "Asiya", "Aylin", "Aytoldi",
    
    # B
    "Bahora", "Barnoxon", "Barno", "Begim", "Begoyim", "Bibigul",
    "Bibixon", "Bozorgul", "Bonu", "Bunyodaxon",
    
    # D
    "Dildora", "Dildoraxon", "Dilnoza", "Dilnozaxon", "Dilrabo",
    "Dilshoda", "Dilobar", "Dilorom", "Diloromxon",
    
    # F
    "Farangiz", "Farida", "Faridaxon", "Farzona", "Farzonaxon",
    "Fatima", "Fatimaxon", "Feruzaxon", "Feruza", "Fotima", "Fotimaxon",
    
    # G
    "Gavhar", "Gavharxon", "Gozal", "Gozalxon", "Guli", "Gulandom",
    "Gulbahor", "Gulchehra", "Gulchiroy", "Gulfiza", "Gulhayo",
    "Gulira", "Guliraxon", "Gulmira", "Gulnora", "Gulnoraxon",
    "Gulnoz", "Gulnozaxon", "Gulruh", "Gulshoda", "Gulzoda",
    
    # H
    "Hafiza", "Hafizaxon", "Halima", "Halimaxon", "Hilola", "Hilolaxon",
    "Humayrо", "Husniya", "Husnora",
    
    # I
    "Iqbol", "Iqbolxon", "Iroda", "Irodaxon", "Islomiya",
    
    # J
    "Jamila", "Jamilaxon", "Jasmin", "Jasminaxon",
    
    # K
    "Kamila", "Kamilaxon", "Karima", "Karimaxon", "Komila", "Komilaxon",
    
    # L
    "Laylo", "Layloxon", "Laziza", "Lazizaxon", "Lola", "Lolaxon",
    "Lobar", "Lobarxon",
    
    # M
    "Madina", "Madinaxon", "Mahbuba", "Mahbubaxon", "Mahfuza", "Mahfuzaxon",
    "Mahliyo", "Mahliyoxon", "Mahmuda", "Mahmudaxon", "Malika", "Malikaxon",
    "Maloxat", "Manzura", "Manzuraxon", "Marguba", "Marjona", "Marjonaxon",
    "Maryam", "Maryamxon", "Mastura", "Masturaxon", "Mavjuda", "Mavluda",
    "Mayram", "Mehriniso", "Mehrinisoxon", "Mohichehra", "Mohigul",
    "Mohira", "Mohiraxon", "Mohlaroyim", "Moxinur", "Moxizoda",
    "Muazzam", "Muazzamxon", "Muborak", "Muhlisa", "Muhlisaxon",
    "Muhayyo", "Muhayyoxon", "Munira", "Muniraxon", "Muqaddas",
    "Muslima", "Muslimaxon",
    
    # N
    "Nafosat", "Nafosatxon", "Nafisa", "Nafisaxon", "Nasiba", "Nasibaxon",
    "Navbahor", "Navbahorxon", "Nazira", "Naziraxon", "Nigora", "Nigoraxon",
    "Nilufar", "Nilufarxon", "Niso", "Nisoxon", "Nodira", "Nodiraxon",
    "Noila", "Noilaxon", "Nozima", "Nozimaxon", "Noziya", "Noziyaxon",
    "Nurbonu", "Nurgul", "Nuriya", "Nuriyaxon",
    
    # O
    "Odina", "Odinaxon", "Oftob", "Oftobxon", "Oisha", "Oishaxon",
    "Omina", "Ominaxon", "Oygul", "Oygulxon", "Oynisa", "Oynisaxon",
    "Oysha", "Oyshaxon", "Ozoda", "Ozodaxon",
    
    # P
    "Parizod", "Parizodxon", "Parvina", "Parvinaxon", "Patma", "Patmaxon",
    
    # Q
    "Qamar", "Qamarxon", "Qunduz", "Qunduzxon",
    
    # R
    "Rano", "Ranoxon", "Robiya", "Robiyaxon", "Rohila", "Rohilaxon",
    "Rohat", "Rohatxon", "Roxat", "Roziya", "Roziyaxon", "Ruxsora",
    "Rukhsora", "Ruxsoraxon",
    
    # S
    "Sabina", "Sabinaxon", "Sabrina", "Sabrinaxon", "Sadoqat", "Sadoqatxon",
    "Saida", "Saidaxon", "Sanam", "Sanamxon", "Saodat", "Saodatxon",
    "Sara", "Saraxon", "Sarvinoz", "Sarvinozxon", "Sevinch", "Sevinchxon",
    "Shahnoza", "Shahnozaxon", "Shahlo", "Shahloxon", "Shahzoda", "Shahzodaxon",
    "Shakhnoza", "Shaxlo", "Shaxnoza", "Shoira", "Shoiraxon",
    "Shohida", "Shohidaxon", "Sitora", "Sitoraxon", "Sohiba", "Sohibaxon",
    "Surayyo", "Surayyoxon", "Suriya", "Suriyaxon",
    
    # T
    "Tahira", "Tahiraxon", "Tamara", "Tamaraxon", "Tanzila", "Tanzilaxon",
    "Turgunoy", "Tursunoy",
    
    # U
    "Umida", "Umidaxon",
    
    # V
    "Vasila", "Vasilaxon", "Venera", "Veneraxon",
    
    # X
    "Xadicha", "Xadichaxon", "Xalima", "Xalimaxon", "Xayitgul",
    "Xilola", "Xilolaxon", "Xonzoda", "Xonzodaxon", "Xurshida", "Xurshidaxon",
    
    # Y
    "Yassaman", "Yulduz", "Yulduzxon",
    
    # Z
    "Zahra", "Zahraxon", "Zarifa", "Zarifaxon", "Zebiniso", "Zebinisoxon",
    "Zilola", "Zilolaxon", "Ziyoda", "Ziyodaxon", "Zulfiya", "Zulfiyaxon",
    "Zumrad", "Zumradxon",
]

# ============================================================
# KOMBINATSIYA - Barcha ismlar
# ============================================================
ALL_UZBEK_NAMES = UZBEK_MALE_NAMES + UZBEK_FEMALE_NAMES

# Ismlarni unikal qilish
ALL_UZBEK_NAMES_SET = set(ALL_UZBEK_NAMES)

# Ismlarning lowercase versiyasi (tez qidirish uchun)
ALL_UZBEK_NAMES_LOWER = {name.lower(): name for name in ALL_UZBEK_NAMES}


def get_all_names() -> list[str]:
    """Barcha O'zbek ismlarini qaytaradi"""
    return ALL_UZBEK_NAMES


def get_names_prompt(max_names: int = 100) -> str:
    """Whisper prompt uchun ismlar ro'yxatini qaytaradi"""
    # Eng ko'p ishlatiladigan ismlarni olish
    common_names = ALL_UZBEK_NAMES[:max_names]
    return "O'zbek ismlari: " + ", ".join(common_names) + "."


def normalize_name(name: str) -> str | None:
    """Ismni normallashtirish - noto'g'ri yozilgan ismni to'g'risini topish
    
    Args:
        name: Foydalanuvchi kiritgan ism
        
    Returns:
        To'g'ri yozilgan ism yoki None agar topilmasa
    """
    if not name:
        return None
    
    name_lower = name.lower().strip()
    
    # 1. To'g'ridan-to'g'ri moslik
    if name_lower in ALL_UZBEK_NAMES_LOWER:
        return ALL_UZBEK_NAMES_LOWER[name_lower]
    
    # 2. O'zbek suffiks olib tashlash (-ni, -ning, -ga, -da, -dan, -lar)
    suffixes = ['ning', 'larni', 'larning', 'larga', 'larda', 'lardan',
                'ni', 'ga', 'da', 'dan', 'lar', 'niki', 'ning']
    
    for suffix in suffixes:
        if name_lower.endswith(suffix) and len(name_lower) > len(suffix) + 2:
            base = name_lower[:-len(suffix)]
            if base in ALL_UZBEK_NAMES_LOWER:
                return ALL_UZBEK_NAMES_LOWER[base]
    
    # 3. Fuzzy matching - birinchi 3-4 harf mos kelsa
    for stored_name, original in ALL_UZBEK_NAMES_LOWER.items():
        if len(name_lower) >= 3 and len(stored_name) >= 3:
            # Birinchi 3 harf mos
            if name_lower[:3] == stored_name[:3]:
                # Uzunlik farqi 3 dan kam
                if abs(len(name_lower) - len(stored_name)) <= 3:
                    return original
    
    return None


def find_similar_names(name: str, max_results: int = 5) -> list[str]:
    """O'xshash ismlarni topish
    
    Args:
        name: Qidirilayotgan ism
        max_results: Maksimal natijalar soni
        
    Returns:
        O'xshash ismlar ro'yxati
    """
    if not name:
        return []
    
    name_lower = name.lower().strip()
    results = []
    
    for stored_name, original in ALL_UZBEK_NAMES_LOWER.items():
        # Birinchi harflar mos kelsa
        if stored_name.startswith(name_lower[:2]) if len(name_lower) >= 2 else False:
            results.append(original)
        # Yoki ism ichida qism sifatida mavjud
        elif name_lower in stored_name or stored_name in name_lower:
            results.append(original)
        
        if len(results) >= max_results:
            break
    
    return results


# ============================================================
# TEST
# ============================================================
if __name__ == "__main__":
    print(f"Jami erkak ismlari: {len(UZBEK_MALE_NAMES)}")
    print(f"Jami ayol ismlari: {len(UZBEK_FEMALE_NAMES)}")
    print(f"Jami barcha ismlar: {len(ALL_UZBEK_NAMES)}")
    print(f"Unikal ismlar: {len(ALL_UZBEK_NAMES_SET)}")
    print()
    print("Test - normalize_name:")
    print(f"  'yodgorbekni' -> {normalize_name('yodgorbekni')}")
    print(f"  'muhammadning' -> {normalize_name('muhammadning')}")
    print(f"  'moxizoda' -> {normalize_name('moxizoda')}")
    print()
    print("Test - find_similar_names('sar'):")
    print(f"  {find_similar_names('sar')}")
