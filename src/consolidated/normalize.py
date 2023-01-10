import unidecode
from itertools import combinations
import networkx as nx
from difflib import SequenceMatcher
from nltk.translate.bleu_score import sentence_bleu


def norm_website(x):
    if isinstance(x, str):
        x = x.strip().lower()
        if len(x) > 7:
            return x.replace("https://", "").replace("http://", "").replace("www.", "")


def norm_areas(s):
    if not isinstance(s, (list, tuple)):
        return s
    areas = ['Sơn La', 'Đắk Lắk', 'Vĩnh Long', 'Tiền Giang', 'Thừa Thiên-Huế',
             'Lâm Đồng', 'Bình Thuận', 'Ninh Thuận', 'Bến Tre', 'Hải Dương',
             'Bắc Giang', 'Bà Rịa - Vũng Tàu', 'An Giang', 'Gia Lai', 'Yên Bái',
             'Lào Cai', 'ĐBSCL', 'Hưng Yên', 'Đồng Nai', 'Quảng Ninh',
             'Sóc Trăng', 'Hà Nam', 'Lạng Sơn', 'Thanh Hóa', 'Hà Giang',
             'Quảng Ngãi', 'Kon Tum', 'Quốc tế', 'Huế', 'Phú Thọ', 'Hà Tĩnh',
             'Điện Biên', 'Tuyên Quang', 'Khánh Hòa', 'Phú Yên', 'Tây Ninh',
             'Nam Định', 'Cần Thơ', 'Cà Mau', 'Hồ Chí Minh', 'Bạc Liêu',
             'Bắc Ninh', 'Thái Nguyên', 'Vĩnh Phúc', 'Bình Phước', 'Đồng Tháp',
             'Thái Bình', 'Bình Dương', 'Bắc Kạn', 'Hà Tây', 'Quảng Trị',
             'Quảng Bình', 'Lai Châu', 'Nghệ An', 'Trà Vinh', 'Đắk Nông',
             'Kiên Giang', 'Biên Hòa', 'Hà Nội', 'Quảng Nam', 'Đà Nẵng',
             'Long An', 'Ninh Bình', 'Hải Phòng', 'Hậu Giang', 'Hòa Bình',
             'Khác', 'Bình Định']
    s = list(map(lambda x: x.strip(), s))
    for ix, e in enumerate(s):
        if e in areas:
            continue
        if unidecode.unidecode(e) == e:
            s[ix] = "Quốc tế"
        else:
            s[ix] = "Khác"
    return s


def build_network_duplicates(
        company_id, ids, token_names, areas,
        level, description,
        weight=(0.2, 0.3, 0.3, 0.2), threshold_score=0.8):
    #     ids = x['id']
    #     token_names = x['token_name']
    #     areas = x['areas']
    #     level = x['level']
    #     company_id = x['company_id']
    #     description = x['description']
    g = nx.Graph()
    for p1, p2 in combinations(zip(ids, token_names, areas, level, description), 2):
        if len(p1[1]) > len(p2[1]):
            hypothesis = p1[1]
            reference = p2[1]
        else:
            hypothesis = p2[1]
            reference = p1[1]
        score_name = sentence_bleu([reference], hypothesis, weights=(0.7, 0.3, 0, 0))

        if len(p1[2]) > len(p2[2]):
            hypothesis = p1[2]
            reference = p2[2]
        else:
            hypothesis = p2[2]
            reference = p1[2]

        score_area = sentence_bleu([reference], hypothesis, weights=(1, 0, 0, 0))
        score_level = 1 if p1[3] == p2[3] else 0
        m = SequenceMatcher(None, p1[4], p2[4])
        score_description = m.ratio()
        score = score_name * weight[0] + score_area * weight[1] \
            + score_level * weight[2] + score_description * weight[3]

        if score > threshold_score:
            g.add_edge(p1[0], p2[0])

    # nx.draw(G, with_labels=True, font_weight='bold')
    lst_components = list(nx.connected_components(g))
    rs = dict()
    for ix, component in enumerate(lst_components):
        for node_ix in component:
            rs[node_ix] = "{}_{}".format(company_id, ix)
    return rs


def build_network_duplicates_v2(
        ids, token_names, areas,
        level, description,
        weight=(0.2, 0.3, 0.3, 0.2)):
    rs = dict()
    for p1, p2 in combinations(zip(ids, token_names, areas, level, description), 2):
        if len(p1[1]) > len(p2[1]):
            hypothesis = p1[1]
            reference = p2[1]
        else:
            hypothesis = p2[1]
            reference = p1[1]
        score_name = sentence_bleu([reference], hypothesis, weights=(0.7, 0.3, 0, 0))

        if len(p1[2]) > len(p2[2]):
            hypothesis = p1[2]
            reference = p2[2]
        else:
            hypothesis = p2[2]
            reference = p1[2]

        score_area = sentence_bleu([reference], hypothesis, weights=(1, 0, 0, 0))
        score_level = 1 if p1[3] == p2[3] else 0
        m = SequenceMatcher(None, p1[4], p2[4])
        score_description = m.ratio()
        score = score_name * weight[0] + score_area * weight[1] \
                + score_level * weight[2] + score_description * weight[3]
        if score > 0.7:
            rs[(p1[0], p2[0])] = score

    return rs
