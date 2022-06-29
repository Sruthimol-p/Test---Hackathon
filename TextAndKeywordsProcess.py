import string
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


class TextAndKeywords:

    def TextAndKeywordsProcessor(self, cur, dunsnum):
        try:

            cur.execute("select webdatatext from webdata where dunsnum = '" + dunsnum + "'")
            records = cur.fetchall()
            #print("select webdatatext from webdata where dunsnum = '" + dunsnum + "'")
            webdata = ''
            for row in records:
                webdata = webdata + " " + row[0]
            #print(webdata)

            tokenized_word = word_tokenize(webdata)
            #print(tokenized_word)

            lowercase_text = [word.lower() for word in tokenized_word]
            #print(lowercase_text)

            stop_words = set(stopwords.words("english"))
            #print(stop_words)

            stop_exclude_words = ['his', 'her', 'she', 'he', 'him', 'hers', 'herself', 'himself']
            for word in stop_exclude_words:
                stop_words.remove(word)
            # print(stop_words)

            s = set(string.punctuation)
            # print(s)

            filtered_word = []
            for i in lowercase_text:
                if i not in s:
                    filtered_word.append(i)
            # print(filtered_word)

            filtered_word_final = []
            for i in filtered_word:
                if i not in stop_words:
                    filtered_word_final.append(i)

            porter_stemmer = nltk.PorterStemmer()
            filtered_word_final.extend(list(set([porter_stemmer.stem(each) for each in filtered_word_final])))
            # print("result of stemming rd: ",Stemmer_roots)

            lemma = nltk.WordNetLemmatizer()
            filtered_word_final.extend(list(set([lemma.lemmatize(each) for each in filtered_word_final])))
            # print("result of lemma: ",lemma_roots)

            final_words = list(set(filtered_word_final))
            #print("final_words------",final_words)

            ## matcher

            # keywords = ['american', 'asian', 'lgbtq', 'women', 'his', 'her', 'hers']
            keywords = []
            cur.execute("select distinct keywords from keywordmapping")
            keyword_records = cur.fetchall()
            for row in keyword_records:
                keywords.append(row[0])

            keywords_matched = []
            lowercase_keywords = [word.lower() for word in keywords]
            '''
            for keyword in lowercase_keywords:
                if keyword in final_words:
                    keywords_matched.append(keyword)
            keywords_text = ("', '".join(keywords_matched))
            '''
            splitkeyword = []
            for keyword in lowercase_keywords:
                splitkeyword = keyword.split()
                # print(splitkeyword)
                count = 1
                if len(splitkeyword) > 1:
                    for word in splitkeyword:
                        if word in final_words:
                            if count == len(splitkeyword):
                                keywords_matched.append(keyword)
                            count = count + 1
                            continue
                else:
                    if keyword in final_words:
                        keywords_matched.append(keyword)
            keywords_text = ("', '".join(keywords_matched))

            #print("keywords_text-----",keywords_text)

            diversity_dimension_query = "select  dd.diversitydimension_name from  diversitydimension dd join keywordmapping km  on km.diversitydimension_id = dd.diversitydimension_id where lower(km.keywords) in ('" + keywords_text + "')"

            #print(diversity_dimension_query)

            cur.execute(diversity_dimension_query)
            diversity_dimension_records = cur.fetchall()
            diversity_dimension_records = list(set(diversity_dimension_records))

            diversity_dimension = ''
            for row in diversity_dimension_records:
                diversity_dimension = diversity_dimension + ", " + row[0]
            diversity_dimension = diversity_dimension[2:]
            #print("---------truncate diversity_dimension--------"+diversity_dimension)
            '''
            if(len(diversity_dimension_records)==1):
                diversity_dimension = diversity_dimension.replace(",", "")
            '''

            #print("diversity_dimension-----",diversity_dimension)
            if (len(diversity_dimension) == 0):
                diversity_dimension = "No Diversity Led Business Found. Requires Manual Check"

            update_desc_query = "update duns set minorityowneddesc='" + diversity_dimension + "' where dunsnum = '" + dunsnum + "'"
            cur.execute(update_desc_query)
            #print(update_desc_query)

            if ('Women'.lower() in diversity_dimension.lower()):
                flag = '1'
            else:
                flag = '0'

            update_women_query = "update duns set iswomenowned="+flag+" where dunsnum = '" + dunsnum + "'"
            cur.execute(update_women_query)
            #print(update_women_query)

        except Exception as error:
            print("Exception occured during TextAndKeywordsProcessor!",error)
