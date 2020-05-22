import time

import language_senario
import retweet_senario
while True:
    language_senario.main();
    retweet_senario.main();
    time.sleep(60000) #1 hour