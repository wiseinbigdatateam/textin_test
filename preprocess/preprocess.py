from es import from_es
from konlpy.tag import *
import pandas as pd
from .variable import pattern_dict, pos_dict, stopwords
import itertools
from collections import Counter
from tabulate import tabulate

class Preprocessor:

    def __init__(self):
        # dataframe
        self.df = None

        # 형태소 분석기
        self.func_name = None

        self.func = None

        # 생성되는 데이터 프레임들을 기억하는 부분
        self.columns = None

        self.select_column = None

        # dict에 각 dataframe 컬럼명으로 데이터를 저장
        self.dict = {}


    # preprocess 총괄 함수
    def start_preprocess(self, func_name, options=None, keywords=None):
        if self.df == None:
            self.get_df()

        # 선택한 형태소 분석기 생성
        self.func_name = func_name
        self.select_func(self.func_name)

        _more_preprocess = True

        while _more_preprocess:
            self.clean(options, keywords)
            close_num = int(input("다른 데이터로 전처리를 진행하실꺼면 1, 종료는 2 : "))
            if close_num == 2: _more_preprocess == False

    # elasticsearch로 부터 dataframe을 갖고 옴
    def get_df(self):
        fes = from_es.FromES()
        dict_df = fes.get_data()
        print(*dict_df.keys(), sep="\n")
        print("원하는 dataframe 선택:")
        df_name = input()
        self.df = dict_df[df_name]

    def _input_options(self):
        print("# 1 : 특수문자  /  2 : 영문  / 3 : 숫자  /  4 : 해당 키워드 삭제  /  5 : 해당 키워드가 있는 문서 삭제")
        options = list(map(int, input("하고자 하는 옵션 :").split(",")))
        print(options)
        if 4 in options or 5 in options:
            keywords = input("삭제할 keyword :").split(',')
        else: keywords = None
        return options, keywords

    def clean(self, options=None, keywords=None):

        if not self.dict:
            self.columns = [self.df.columns[0]]
            self.df = pd.DataFrame(self.df[self.columns])
        else:
            self.columns = list(self.dict.keys())
            print("현재 갖고 있는 dataframe :", self.columns)
            _col = input("다시 전처리를 할 dataframe 선택 : ")
            self.df = self.dict[_col]
            options, keywords = self._input_options()

        _name = 'preprocess_'

        choice = True

        while choice:

            # 처음 입력받은 dataframe 정제 => null값 제거
            self.delete_field()

            if len(self.dict) <= 1:
                _col_num = 1
                self.dict[_name + str(_col_num)] = self.df
                # self.columns.append(_name + str(_col_num))
                self._create_df(_col_num)

            # list(filter(lambda x: 'test' in x, tmp))[0].split('_')[-1]
            else:
                _col_list = self.columns
                _col_list.sort(reverse=True)
                _col_num = int(list(filter(lambda x: _name in x, _col_list))[0].split("_")[-1])
                # self.columns.append(_name + str(_col_num+1))
                self._create_df(_col_num)

            # 옵션에 따른 전처리 실행
            # 1 : 특수문자  /  2 : 영문  / 3 : 숫자  /  4 : 해당 키워드 삭제  /  5 : 해당 키워드가 있는 문서 삭제
            if options != None:
                print(options)
                self.clean_text(options, keywords)

            self.dict[self.select_column] = self.df

            print(_col_num, "차 전처리 완료")
            print(self.dict[self.select_column])
            print("추가 전처리 1 / 종료 2")
            if input() == '2':
                choice = False
            else:
                print("column list :", self.columns)
                self.select_column = input("수정할 Column, Default는 최근 전처리한 column :")
                self.df = self.dict[self.select_column]
                options, keywords = self._input_options()


        morph_options = list(map(str, input("추출하고자 하는 형태소, 1:명사, 2:동사, 3:형용사 :").split(",")))

        self.get_morph(morph_options)

    def _create_df(self, col_num):
        # self.columns.append(_name + str(_col_num+1))
        print("nameeeeeeeeeeeeeeeeeeeeeeeeeeee : ", self.select_column)
        if len(self.dict) != 1:
            print(self.dict[self.select_column])
            print("*************************************************")
            self.select_column = 'preprocess_' + str(col_num+1)

        tmp_df = pd.DataFrame(self.df)
        tmp_df.columns = [self.select_column]
        self.df = tmp_df.dropna().reset_index(drop=True)
        self.dict[self.select_column] = self.df
        self.columns = list(self.dict.keys())
        self.df = self.dict[self.select_column]

    def clean_text(self, options, keywords=None):
        print("********************clean_text********************")
        self.df = self.dict[self.select_column]
        if len(options) > 1:
            for num in options: self.del_pattern(num, keywords)
        else:
            self.del_pattern(options[0], keywords)

    # Null값 제거, keyword 리스트가 있을 경우 해당 키워드가 있는 문서도 제거
    def delete_field(self, keywords=None):

        if self.select_column == None:
            self.select_column = self.df.columns[-1]

        # 키워드가 여러개 있을 경우 list로 받음
        if type(keywords) == list:
            for keyword in keywords:
                self.df = self.df[self.df[self.select_column].str.contains(keyword) == False]

        # 키워드 1개만 삭제할 경우
        elif type(keywords) == str:
            self.df = self.df[self.df[self.select_column].str.contains(keywords) == False]

        # 키워드가 삭제되면서 문서가 null이 될 수도 있기에 dropna를 항상 함
        self.df = self.df.dropna().reset_index(drop=True)

    # 형태소 분석기 선택
    def select_func(self, func_name):
        if func_name == 'okt':
            self.func = Okt()
        else:
            self.func = Mecab("../data/mecab/mecab-ko-dic")

    # 특정 키워드 제거
    def del_pattern(self, num, keywords):
        print("**********************del_pattern****************************")
        print(self.select_column)
        print(keywords)
        print(self.columns)
        if num < 4:
             self.df[self.select_column] = self.df[self.select_column].str.replace(pattern_dict[num], ' ', regex=True)

        elif num == 4 and keywords != None:
            print(keywords)
            for keyword in keywords:
                self.df[self.select_column] = self.df[self.select_column].str.replace(keyword, ' ')

        # 옵션 5 : 해당 키워드가 있는 문서 삭제
        elif num == 5 and keywords != None:
            self.delete_field(keywords)

        self.df[self.select_column] = self.df[self.select_column].str.replace(' +', ' ', regex=True)
        self.dict[self.select_column] = self.df

    def get_morph(self, options):
        print(self.select_column)
        print(self.df)
        morph_list = list(itertools.chain.from_iterable([pos_dict[self.func_name][num] for num in options]))
        print(morph_list)

        extract_morph_words = []
        for i, text in enumerate(self.df[self.select_column]):
            try:
                if self.func_name == 'okt':
                    tmp_list = list(list(zip(*list(filter(lambda x: x[-1] in morph_list and x[0] not in stopwords and len(x[0])>1, self.func.pos(text, norm=True, stem=True)))))[0])
                else: tmp_list = list(list(zip(*list(filter(lambda x: x[-1] in morph_list and x[0] not in stopwords and len(x[0])>1, self.func.pos(text)))))[0])

            except: tmp_list = []

            finally: extract_morph_words.append(tmp_list)

        morph_name = 'morph_' + self.select_column

        extract_morph_words = [' '.join(word) for word in extract_morph_words]

        self.df = pd.DataFrame(extract_morph_words)
        self.df.columns = [self.func_name]
        self.dict[self.func_name] = self.df
        self._get_word_freq()
        # self.dict[self.func_name].to_csv(self.func_name + "_형태소 추출 테스트 결과.csv", index=False)

    def _get_word_freq(self):
        _word_frequency = Counter(' '.join(list(self.df[self.func_name])).split(" ")).most_common(100)
        print(tabulate(_word_frequency, headers=["Word", "Frequncy"]))


# if __name__ == "__main__":
#     ps = Preprocessor()
#     # test_df = pd.read_csv("../../전처리 테스트/test_data.csv")
#     keywords = ['경기도', '석촌', '아파트', '부동산', '서울']
#     save_df = ps.clean('mecab', [1,2,3,5], keywords)