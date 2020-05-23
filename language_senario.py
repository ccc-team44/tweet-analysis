import couchdb
import pandas as pd
import json

# Translate the language code to full language name.
translate = {'en': 'English', 'ar': 'Arabic', 'bn': 'Bengali', 'cs': 'Czech', 'da': 'Danish', 'de': 'German',
             'el': 'Greek ', 'es': 'Spanish', 'fa': 'Persian', 'fi': 'Finnish', 'fil': 'Filipino', 'fr': 'French',
             'he': 'Hebrew', 'hi': 'Hindi', 'hu': 'Hungarian', 'in': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese',
             'ko': 'Korean', 'msa': 'Malay', 'nl': 'Dutch', 'no': 'Norwegian', 'pl': 'Polish', 'pt': 'Portuguese',
             'ro': 'Romanian', 'ru': 'Russian', 'sv': 'Swedish', 'th': 'Thai', 'tr': 'Turkish', 'uk': 'Ukrainian',
             'ur': 'Urdu', 'vi': 'Vietnamese', 'zh': 'Chinese', 'am': 'Amharic', 'is': 'Icelandic', 'hy': 'Armenian',
             'sr': 'Serbian', 'bg': 'Bulgarian', 'sd': 'Sindhi', 'my': 'Burmese', 'kn': 'Kannada', 'si': 'Sinhala',
             'km': 'Khmer', 'sk': 'Slovak', 'cs': 'Czech', 'sl': 'Slovenian', 'lo': 'Lao', 'ckb': 'Sorani Kurdish',
             'nl': 'Dutch', 'lv': 'Latvian', 'lt': 'Lithuanian', 'et': 'Estonian', 'ml': 'Malayalam', 'tl': 'Tagalog',
             'dv': 'Maldivian', 'ta': 'Tamil', 'mr': 'Marathi', 'te': 'Telugu', 'ka': 'Georgian',
             'ne': 'Nepali', 'bo': 'Tibetan', 'or': 'Oriya', 'gu': 'Gujarati', 'pa': 'Panjabi',
             'ht': 'Haitian', 'ps': 'Pashto', 'iw': 'Hebrew', 'ug': 'Uyghur', 'cy': 'Welsh', 'und': 'other'}


def read_json(file_name):
    with open(file_name, 'r') as json_file:
        aurindata = json.load(json_file)
    return aurindata


# Rename and reformat the aurin data.
def aurin_data_reformat(aurin_data):
    # create a new dict
    format_data = {}
    # formatted the dict {lga_code:{lang:freq}} to make analysis easier and convenient
    for each in aurin_data['features']:
        format_data[each['properties']['lga_code_2016']] = {
            'bulgarian': int(each['properties']['person_spks_macedonian_tot']),
            'polish': int(each['properties']['person_spks_polish_tot']),
            'russian': int(each['properties']['person_spks_russian_tot']),
            'english': int(each['properties']['person_tot_spks_eng_only']),
            'italian': int(each['properties']['person_spks_italian_tot']),
            'maltese': int(each['properties']['person_spks_maltese_tot']),
            'panjabi': int(each['properties']['person_spks_indo_aryan_lang_punjabi_tot']),
            'bengali': int(each['properties']['person_spks_indo_aryan_lang_bengali_tot']),
            'tagalog': int(each['properties']['person_spks_se_asia_austronesia_lang_tagalog_tot']),
            'vietnamese': int(each['properties']['person_spks_vietnamese_tot']),
            'korean': int(each['properties']['person_spks_korean_tot']),
            'indonesian': int(each['properties']['person_spks_se_asia_austronesia_lang_indonesian_tot']),
            'filipino': int(each['properties']['person_spks_se_asia_austronesia_lang_filipino_tot']),
            'greek': int(each['properties']['person_spks_greek_tot']),
            'spanish': int(each['properties']['person_spks_spanish_tot']),
            'thai': int(each['properties']['person_spks_thai_tot']),
            'tamil': int(each['properties']['person_spks_tamil_tot']),
            'serbian': int(each['properties']['person_spks_serbian_tot']),
            'sinhala': int(each['properties']['person_spks_indo_aryan_lang_sinhalese_tot']),
            'turkish': int(each['properties']['person_spks_turkish_tot']),
            'hindi': int(each['properties']['person_spks_indo_aryan_lang_hindi_tot']),
            'persian': int(each['properties']['person_spks_persian_ed_tot']),
            'urdu': int(each['properties']['person_spks_indo_aryan_lang_urdu_tot']),
            'japanese': int(each['properties']['person_spks_japanese_tot']),
            'samoan': int(each['properties']['person_spks_samoan_tot']),
            'other': int(each['properties']['person_spks_lang_oth_than_eng_tot'] + each['properties'][
                'person_spks_lang_oth_than_eng_tot'] - each['properties']['person_spks_macedonian_tot'] -
                         each['properties']['person_spks_polish_tot'] - each['properties']['person_spks_russian_tot'] -
                         each['properties']['person_spks_italian_tot'] - each['properties']['person_spks_maltese_tot'] -
                         each['properties']['person_spks_indo_aryan_lang_punjabi_tot'] - each['properties'][
                             'person_spks_indo_aryan_lang_bengali_tot'] - each['properties'][
                             'person_spks_se_asia_austronesia_lang_tagalog_tot'] - each['properties'][
                             'person_spks_vietnamese_tot'] - each['properties']['person_spks_korean_tot'] -
                         each['properties']['person_spks_se_asia_austronesia_lang_indonesian_tot'] - each['properties'][
                             'person_spks_se_asia_austronesia_lang_filipino_tot'] - each['properties'][
                             'person_spks_greek_tot'] - each['properties']['person_spks_spanish_tot'] -
                         each['properties']['person_spks_thai_tot'] - each['properties']['person_spks_tamil_tot'] -
                         each['properties']['person_spks_serbian_tot'] - each['properties'][
                             'person_spks_indo_aryan_lang_sinhalese_tot'] - each['properties'][
                             'person_spks_turkish_tot'] - each['properties']['person_spks_indo_aryan_lang_hindi_tot'] -
                         each['properties']['person_spks_persian_ed_tot'] - each['properties'][
                             'person_spks_indo_aryan_lang_urdu_tot'] - each['properties']['person_spks_japanese_tot'] -
                         each['properties']['person_spks_samoan_tot'])}
    return format_data


def lga_code_to_region(code):
    # By the rule of LOCAL GOVERNMENT AREAS, each state has its country code from 1-7.
    # There are only 7 states here(8 states now), as in 2016, the 'Australian Capital Territory' is not defined in LGA.
    if str(code).startswith('1'):
        return 'New South Wales'
    elif str(code).startswith('2'):
        return 'Victoria'
    elif str(code).startswith('3'):
        return 'Queensland'
    elif str(code).startswith('4'):
        return 'South Australia'
    elif str(code).startswith('5'):
        return 'Western Australia'
    elif str(code).startswith('6'):
        return 'Tasmania'
    else:
        return 'Northern Territory'


def aurin_data_analysis(filename):
    aurin_data = read_json(filename)
    format_data = aurin_data_reformat(aurin_data)
    format_data = pd.DataFrame(format_data).T
    format_data = format_data.reset_index()
    format_data['index'] = format_data['index'].apply(lga_code_to_region)
    aurin = format_data.groupby(['index']).sum()
    aurin_total = aurin.sum(axis=1)
    aurin_final = aurin.div(aurin_total, axis='rows')
    return aurin_final


def connect_db(url):
    try:
        couch = couchdb.Server(url)
        return couch
    except:
        print('error')


def get_view(database_name, view_name, couch):
    try:
        db = couch[database_name]
        res = db.view(view_name, group='true')
        return res
    except:
        print('Error. No such view.')


def loc_to_state(filename='au.json'):
    audata = read_json(filename)
    state = {}
    for info in audata:
        admin = info['admin']
        if admin in state.keys():
            state[admin].append(info['city'])
        else:
            state[admin] = []
            state[admin].append(info['city'])
    return state


def find_loc(address):
    state = loc_to_state()
    for state_name in state.keys():
        if address.find(state_name) != -1:
            return state_name

        for city_name in state[state_name]:
            if address.find(city_name) != -1:
                return state_name
    return None


def reformat_twitter_data(view, aurin_data):
    df = pd.DataFrame(view)
    df['city'] = df['key'].apply(lambda x: x[0])
    df['lang'] = df['key'].apply(lambda x: x[1])
    df['lang'] = df['lang'].replace(translate)
    df['lang'] = df['lang'].apply(lambda x: x.lower())
    df['location'] = df['city'].apply(find_loc)
    del df['key']
    del df['city']
    df.dropna(axis=0, how='any', inplace=True)

    # Set language not in aurin data as 'other'
    for each in df['lang'].unique():
        if each.lower() not in aurin_data.keys():
            df.loc[df['lang'] == each, 'lang'] = 'other'

    return df


def tweets_analysis(df):
    total = df.groupby(['location'])['value'].sum()
    stat = df.groupby(['location', 'lang'])['value'].sum().reindex()
    percentage_data = stat / total
    percentage_data.fillna(0, inplace=True)
    tweet_final = pd.DataFrame(percentage_data)
    return tweet_final


def output_result(aurin_final, tweet_final):
    out = {}

    for (loc, lang) in tweet_final.index:
        try:
            if loc not in out.keys():
                out[loc] = []
                out[loc].append({
                    'label': lang,
                    'twitter': '%.4f' % (tweet_final.loc[loc, lang].value * 100),
                    'aurin': '%.4f' % (aurin_final.loc[loc, lang] * 100)
                })

            else:
                out[loc].append({
                    'label': lang,
                    'twitter': '%.4f' % (tweet_final.loc[loc, lang].value * 100),
                    'aurin': '%.4f' % (aurin_final.loc[loc, lang] * 100)
                })
        except:
            if loc not in out.keys():
                out[loc] = []
                out[loc].append({
                    'label': lang,
                    'twitter': '%.4f' % (tweet_final.loc[loc, lang].value * 100),
                    'aurin': '0.0'
                })

            else:
                out[loc].append({
                    'label': lang,
                    'twitter': '%.4f' % (tweet_final.loc[loc, lang].value * 100),
                    'aurin': '0.0'
                })
    return out

def save_tweet(name, newdata, couch):
    try:
        database = couch[name]
        newdata['_id'] = 'lang'
        newdata['_rev'] = database['lang'].rev
        database.save(newdata)
    except:
        print("Creating database", name)
        database = couch.create(name)
        newdata['_id'] = 'lang'
        database.save(newdata)


if __name__ == '__main__':
    print('*********************Aurin Data Analysis*********************************')
    aurin_final = aurin_data_analysis('LGA language spoken.json')
    print('*********************Connecting to DataBase*********************************')
    url = 'http://admin:1111@172.26.130.31:5984/'
    couch = connect_db(url)
    print('*********************Tweets Data Analysis*********************************')
    view = get_view('tweets', "sentiment/lang", couch)
    df = reformat_twitter_data(view, aurin_final)
    tweet_final = tweets_analysis(df)
    output = output_result(aurin_final, tweet_final)
    print('*********************Output*********************************')
    save_tweet('lang_output', output, couch)

