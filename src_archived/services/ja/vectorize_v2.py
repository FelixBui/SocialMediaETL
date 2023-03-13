import tensorflow as tf
import numpy as np


def vectorize_text(text, vocab_table, max_sequence_length=128):
    text_tokenized = tf.strings.split(text, sep=' ')
    # Index lookup
    text_vectorized = vocab_table.lookup(text_tokenized)
    text_vectorized = text_vectorized[:max_sequence_length] + 1

    # Pad to max length
    text_paddings = tf.concat(([[0, max_sequence_length-tf.shape(text_vectorized)[0]]],), axis=0)
    text_vectorized = tf.pad(text_vectorized, text_paddings)

    return text_vectorized


def transform_text_input(text_input, vocab_table, max_text_length=128):
    # Transform text input
    text_vectorized = vectorize_text(text_input, vocab_table=vocab_table, max_sequence_length=max_text_length)
    return text_vectorized


def vectorize_categorical(value, num_class):
    return tf.one_hot(value % num_class, num_class)


# warning sep
def transform_categorical_input(salary, gender, degree, fields,
                                num_fields=57, num_degree=8, num_salary=13, num_gender=3, sep=','):
    salary_vectorized = vectorize_categorical(salary, num_class=num_salary)
    gender_vectorized = vectorize_categorical(gender, num_class=num_gender)
    degree_vectorized = vectorize_categorical(degree, num_class=num_degree)
    # multi-label value
    fields_vectorized = tf.strings.split(fields, sep=sep)
    fields_vectorized = tf.strings.to_number(fields_vectorized, tf.int32)
    fields_vectorized = vectorize_categorical(fields_vectorized, num_class=num_fields)
    fields_vectorized = tf.math.reduce_sum(fields_vectorized, axis=0)
    categorical_input_vectorized = tf.concat([salary_vectorized, gender_vectorized, degree_vectorized,
                                              fields_vectorized], axis=0)
    return categorical_input_vectorized


def tf_data_set(dataframe, vocab_table=None, mode='text', num_parallel=None):
    """
    Function to create an data generator from data to feed into feature extractor model
    :param dataframe: a Panda dataframe contains preprocessed data
    :param vocab_table: vocabulary for word embedding
    :param mode: 'text'/'title'/'cat', 'text' for long text, 'title' for title, 'cat' for category
    :param num_parallel:
    :return:
    """
    if num_parallel is None:
        tf_num_parallel = tf.data.AUTOTUNE
    else:
        tf_num_parallel = tf.constant(num_parallel, dtype=tf.int64)
    if mode == 'text':
        max_text_length = 128
    elif mode == 'title':
        max_text_length = 10

    if mode == 'text' or mode == 'title':
        tf_data = tf.data.Dataset.from_tensor_slices(
            (dataframe['text'].values.astype('str'))
        )
        tf_data = tf_data.map(
            lambda text: transform_text_input(text, vocab_table=vocab_table,
                                              max_text_length=max_text_length,),
            num_parallel_calls=tf_num_parallel
        )
    elif mode == 'cat':
        tf_data = tf.data.Dataset.from_tensor_slices(
            (dataframe['salary'].values, dataframe['gender'].values,
             dataframe['degree'].values, dataframe['fields'].values.astype('str'))
        )
        tf_data = tf_data.map(
            lambda salary, gender, degree, fields:
            transform_categorical_input(salary, gender, degree, fields),
            num_parallel_calls=tf_num_parallel
        )
    else:
        raise KeyError(f"tf_data_set: {mode} mode is not support")

    tf_data = tf_data.map(lambda x: tf.dtypes.cast(x, tf.float64), num_parallel_calls=tf_num_parallel)
    return tf_data


def get_feature(dataframe, model=None, vocab_table=None, mode='text',
                batch=32, prefetch=tf.data.AUTOTUNE):
    """
    Function to get the feature vector from preprocessed data
    :param dataframe: A panda dataframe of preprocessed data as input
    :param model: Model to extract feature vector
    :param vocab_table: Vocabulary file for word embedding
    :param mode: 'text'/'title'/'cat', 'text' for long text, 'title' for title, 'cat' for category
    :param batch:
    :param prefetch:
    :return:
    """
    if mode == 'cat':
        generator = tf_data_set(dataframe, vocab_table, mode=mode, num_parallel=None)
        # arr = []
        # for element in generator.batch(2):
        #     arr.append(element.numpy())
        # feature_output = np.concatenate(arr, axis=0)
        # return feature_output
    elif mode == 'text':
        generator = tf_data_set(dataframe, vocab_table, mode='text', num_parallel=None)
    elif mode == 'title':
        generator = tf_data_set(dataframe, vocab_table, mode='title', num_parallel=None)
    else:
        raise KeyError(f"get_feature: {mode} mode is not support")
    feature_output = model.predict(
        generator.batch(batch).prefetch(prefetch)
    )
    return feature_output
