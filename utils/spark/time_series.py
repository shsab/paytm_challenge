import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.sql.window import Window

def date_range(t1, t2, step=1):
    """
    Return a list of equally spaced points between t1 and t2 with stepsize step.

    :param t1: start epoch
    :param t2: end epoch
    :param step: step size default is one second
    :return:
    """
    return [t1 + step*x for x in range(int((t2-t1)/step)+1)]

# define udf
udf_date_range = sf.udf(date_range, ArrayType(LongType()))

