import tensorflow as tf
import numpy as np
import math
from tensorflow.keras.utils import Sequence


class DataGenerator(Sequence):
    def __init__(
            self,
            data,
            batch_size=32,
            shuffle=False,
    ):
        self.shuffle = shuffle
        self.batch_size = batch_size
        self.data = data
        self.indexes = np.arange(len(self.data))

    def __len__(self):
        # Denotes the number of batches per epoch.
        # return len(self.data) // self.batch_size
        return math.ceil(len(self.data) / self.batch_size)

    def __getitem__(self, idx):
        indexes = self.indexes[idx * self.batch_size: (idx + 1) * self.batch_size]
        data = self.data[indexes]

        return [
            tf.convert_to_tensor(data[:, 0].tolist()),
            tf.convert_to_tensor(data[:, 1].tolist()),
            tf.convert_to_tensor(data[:, 2].tolist()),
            tf.convert_to_tensor(data[:, 3].tolist()),
            tf.convert_to_tensor(data[:, 4].tolist()),
            tf.convert_to_tensor(data[:, 5].tolist())
        ]


def predict_data_generator(dataframe, num_parallel=None):
    def cast_data(x1, x2, x3, x4, x5, x6):
        return ((tf.dtypes.cast(x1, tf.float64), tf.dtypes.cast(x2, tf.float64), tf.dtypes.cast(x3, tf.float64),
                 tf.dtypes.cast(x4, tf.float64), tf.dtypes.cast(x5, tf.float64), tf.dtypes.cast(x6, tf.float64)),)
    if num_parallel is None:
        tf_num_parallel = tf.data.AUTOTUNE
    else:
        tf_num_parallel = tf.constant(num_parallel, dtype=tf.int64)

    tf_data = tf.data.Dataset.from_tensor_slices((dataframe['job_text'].values.tolist(),
                                                  dataframe['job_title'].values.tolist(),
                                                  dataframe['job_category'].values.tolist(),
                                                  dataframe['resume_text'].values.tolist(),
                                                  dataframe['resume_title'].values.tolist(),
                                                  dataframe['resume_category'].values.tolist(),
                                                  ))

    tf_data = tf_data.map(cast_data, num_parallel_calls=tf_num_parallel)
    return tf_data
