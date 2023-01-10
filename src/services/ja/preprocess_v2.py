# -*- coding: utf-8 -*-
import re


def remove_html(txt):
    return re.sub(r'<[^>]*>', '', txt)


uni_chars = "àáảãạâầấẩẫậăằắẳẵặèéẻẽẹêềếểễệđìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵ" \
            "ÀÁẢÃẠÂẦẤẨẪẬĂẰẮẲẴẶÈÉẺẼẸÊỀẾỂỄỆĐÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴÂĂĐÔƠƯ"
unsign_chars = "aaaaaaaaaaaaaaaaaeeeeeeeeeeediiiiiooooooooooooooooouuuuuuuuuuuyyyyy" \
               "AAAAAAAAAAAAAAAAAEEEEEEEEEEEDIIIOOOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYAADOOU"


def load_dic_char():
    dic = {}
    char1252 = 'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|' \
               'ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|' \
               'À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|' \
               'Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ'.split('|')
    char_utf8 = "à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|" \
                "ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|" \
                "À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|" \
                "Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ".split('|')
    for i in range(len(char1252)):
        dic[char1252[i]] = char_utf8[i]
    return dic


dic_char = load_dic_char()


def convert_unicode(txt):
    return re.sub(
        r'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|'
        r'ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|'
        r'À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|'
        r'Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ',
        lambda x: dic_char[x.group()], txt)


def word_tokenize(text, rdrsegmenter):
    stop_words = [
        'bị', 'bởi', 'cả', 'các', 'cái', 'cần', 'càng', 'chỉ', 'chiếc', 'cho', 'chứ', 'chưa', 'chuyện',
        'có', 'có_thể', 'cứ', 'của', 'cùng', 'cũng', 'đã', 'đang', 'đây', 'để', 'đến_nỗi', 'đều',
        'điều', 'do', 'đó', 'được', 'dưới', 'gì', 'khi', 'không', 'là', 'lại', 'lên', 'lúc', 'mà',
        'mỗi', 'một_cách', 'này', 'nên', 'nếu', 'ngay', 'nhiều', 'như', 'nhưng', 'những', 'nơi', 'nữa',
        'phải', 'qua', 'ra', 'rằng', 'rằng', 'rất', 'rất', 'rồi', 'sau', 'sẽ', 'so', 'sự', 'tại', 'theo',
        'thì', 'trên', 'trước', 'từ', 'từng', 'và', 'vẫn', 'vào', 'vậy', 'vì', 'việc', 'với', 'vừa']
    try:
        sentences = rdrsegmenter.tokenize(text)
        test_sentence = " ".join([" ".join([x for x in sentence if x not in stop_words]) for sentence in sentences])
        return test_sentence
    except Exception as _:
        return None


def text_preprocess(document, rdrsegmenter):
    # remove html code
    document = remove_html(document)
    # unicode standardize
    document = convert_unicode(document)
    # đưa về lower
    document = document.lower()
    # remove unecessary
    document = re.sub(r'\\n|\\t|\\r', '. ', document)
    document = re.sub(r'[^\s\wáàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệóòỏõọôốồổỗộơớờởỡợíìỉĩịúùủũụưứừửữựýỳỷỹỵđ_]', ' ', document)
    # remove digits
    document = re.sub(r"\d+", " ", document)
    # remove spaces
    document = re.sub(r'\s+', ' ', document)
    # tokenize
    document = word_tokenize(document, rdrsegmenter)
    document = document.strip() if document is not None else ''
    return document


def preprocess_job_text(job, rdrsegmenter):
    job_text = job[['job_id', 'title', 'skill', 'description']]
    job_text.columns = ['job_id', 'title', 'requirement', 'description']
    job_text['requirement'] = job_text['description'] + ' . ' + job_text['requirement']
    job_text['requirement'] = job_text['requirement'].apply(lambda x: text_preprocess(x, rdrsegmenter))
    job_text['title'] = job_text['title'].apply(lambda x: text_preprocess(x, rdrsegmenter))
    job_text = job_text[['job_id', 'title', 'requirement']]
    return job_text


def fill_num(x):
    if isinstance(x, str):
        if x == '':
            return 0
    elif isinstance(x, float):
        return int(x)
    else:
        return x


def preprocess_job_feature(job):
    feature_job = job[['job_id', 'salary', 'degree', 'gender', 'fields', 'provinces']]
    feature_job['salary'] = feature_job['salary'].map(fill_num)
    feature_job['gender'] = feature_job['gender'].map(fill_num)
    feature_job['degree'] = feature_job['degree'].map(fill_num)
    return feature_job


def preprocess_job(job, rdrsegmenter):
    job_text = preprocess_job_text(job, rdrsegmenter)
    feature_job = preprocess_job_feature(job)
    job_merge = job_text.merge(feature_job[['job_id', 'salary', 'degree',
                                            'gender', 'fields', 'provinces']], on='job_id')
    job_merge.columns = ['job_id', 'job_title', 'requirement',
                         'job_salary', 'job_degree', 'job_gender', 'job_fields', 'job_provinces']
    return job_merge


def preprocess_resume_text(res, res_exp, rdrsegmenter):
    res_title_text = res[['seeker_id', 'resume_id', 'title']]
    res_title_text.title = res_title_text.title.apply(lambda x: text_preprocess(x, rdrsegmenter))

    res_text = res_exp.sort_values(by='start_date', ascending=False)[['resume_id', 'description']]
    res_text['text'] = res_text['description']
    res_text['text'] = res_text.groupby('resume_id')['text'].transform(lambda x: ' . '.join(x))
    res_text = res_text[['resume_id', 'text']].drop_duplicates()
    res_text['text'] = res_text['text'].apply(lambda x: text_preprocess(x, rdrsegmenter))

    res_text = res_text.merge(res_title_text, on=['resume_id'])
    res_text = res_text[['seeker_id', 'resume_id', 'title', 'text']]
    return res_text


def preprocess_resume_feature(res):
    feature_resume = res[['seeker_id', 'resume_id', 'salary', 'degree', 'gender', 'fields', 'provinces']]
    feature_resume['salary'] = feature_resume['salary'].map(fill_num)
    feature_resume['gender'] = feature_resume['gender'].map(fill_num)
    feature_resume['degree'] = feature_resume['degree'].map(fill_num)

    return feature_resume


def preprocess_resume(res, res_exp, rdrsegmenter):
    res_text = preprocess_resume_text(res, res_exp, rdrsegmenter)
    feature_resume = preprocess_resume_feature(res)
    res_merge = res_text.merge(feature_resume[['seeker_id', 'resume_id', 'salary', 'degree',
                                               'gender', 'fields', 'provinces']], on=['seeker_id', 'resume_id'])
    res_merge.columns = ['seeker_id', 'resume_id', 'resume_title', 'text',
                         'resume_salary', 'resume_degree', 'resume_gender', 'resume_fields', 'resume_provinces']
    return res_merge
