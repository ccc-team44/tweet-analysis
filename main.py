import time

import language_senario
import retweet_senario
import morrison_by_month

while True:
    try:
        language_senario.main()
    except Exception as e:
        print('error in language senario', e)
    try:
        retweet_senario.main()
    except Exception as e:
        print('error in retweet senario', e)
    try:
        morrison_by_month.main()
    except Exception as e:
        print('error in morrison senario', e)
    time.sleep(60000)  # 1 hour
