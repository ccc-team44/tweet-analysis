import time

import language_senario
import retweet_senario
import morrison_by_month

while True:
    try:
        language_senario.main()
    except:
        print('error in language senario')
    try:
        retweet_senario.main()
    except:
        print('error in retweet senario')
    try:
        morrison_by_month.main()
    except:
        print('error in morrison senario')
    time.sleep(60000)  # 1 hour
