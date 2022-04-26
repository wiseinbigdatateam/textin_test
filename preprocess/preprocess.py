from es import from_es
from konlpy.tag import *
import pandas as pd
from .variable import pattern_dict, pos_dict, stopwords
import itertools

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

    # elasticsearch로 부터 dataframe을 갖고 옴
    def get_df(self):
        fes = from_es.FromES()
        dict_df = fes.get_data()
        print(*dict_df.keys(), sep="\n")
        df_name = input("원하는 dataframe 선택:")
        self.df = dict_df[df_name]

    # 전처리를 전체적으로 시작
    # func는 형태소 분석기 이름
    def clean(self, func_name, options=None, keywords=None):

        if self.df == None:
            self.get_df()

        self.columns = [self.df.columns[0]]

        # 선택한 형태소 분석기 생성
        self.func_name = func_name
        self.select_func(self.func_name)

        _name = 'preprocess_'

        choice = True

        while choice:

            # 처음 입력받은 dataframe 정제 => null값 제거
            self.delete_field()

            if len(self.dict) <= 1:
                _col_num = 1
                self.dict[self.df.columns[0]] = self.df
                self.columns.append(_name + str(_col_num))
                self.select_column = self.columns[-1]
                tmp_df = pd.DataFrame(self.df[self.columns[0]])
                tmp_df.columns = [self.select_column]
                self.df = tmp_df.dropna().reset_index(drop=True)

            else:
                _col_num = int(self.columns[-1].split('_')[-1]) + 1
                self.columns.append(_name + str(_col_num))
                self.df[self.columns[-1]] = self.df[self.select_column]
                self.select_column = self.columns[-1]
                tmp_df = pd.DataFrame(self.df[self.select_column])
                tmp_df.columns = [self.select_column]
                self.df = tmp_df.dropna().reset_index(drop=True)

            # 옵션에 따른 전처리 실행
            # 1 : 특수문자  /  2 : 영문  / 3 : 숫자  /  4 : 해당 키워드 삭제  /  5 : 해당 키워드가 있는 문서 삭제
            if options != None:
                self.clean_text(options, keywords)

            self.dict[self.select_column] = self.df

            print(_col_num, "차 전처리 완료")
            print(self.dict[self.columns[-2]])
            print(self.dict[self.columns[-1]])
            print("추가 전처리 1 / 종료 2")
            if input() == '2':
                choice = False
            else:
                print("column list :", self.columns)
                self.select_column = input("수정할 Column, Default는 최근 전처리한 column :")
                print("# 1 : 특수문자  /  2 : 영문  / 3 : 숫자  /  4 : 해당 키워드 삭제  /  5 : 해당 키워드가 있는 문서 삭제")
                options = list(map(int, input("하고자 하는 옵션 :").split()))
                keywords = input("삭제할 keyword :")

        morph_options = list(map(str, input("추출하고자 하는 형태소, 1:명사, 2:동사, 3:형용사 :").split()))

        self.get_morph(func_name, morph_options)

        return self.df

    def clean_text(self, options, keywords=None):
        print("********************clean_text********************")
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

        if num < 4:
            self.df[self.select_column] = self.df[self.select_column].replace(pattern_dict[num], '', regex=True)

        elif num == 4 and keywords != None:
            for keyword in keywords:
                self.df[self.select_column] = self.df[self.select_column].str.replace(keyword, '')

        # 옵션 5 : 해당 키워드가 있는 문서 삭제
        elif num == 5 and keywords != None:
            self.delete_field(keywords)

    def get_morph(self, func_name, options):
        print(self.select_column)
        print(self.df)
        morph_list = list(itertools.chain.from_iterable([pos_dict[func_name][num] for num in options]))
        print(morph_list)

        extract_morph_words = []
        for i, text in enumerate(self.df[self.select_column]):
            try:
                if func_name == 'okt':
                    tmp_list = list(list(zip(*list(filter(lambda x: x[-1] in morph_list and x[0] not in stopwords and len(x[0])>1, self.func.pos(text, norm=True, stem=True)))))[0])
                else: tmp_list = list(list(zip(*list(filter(lambda x: x[-1] in morph_list and x[0] not in stopwords and len(x[0])>1, self.func.pos(text)))))[0])

            except: tmp_list = []

            finally: extract_morph_words.append(tmp_list)

        morph_name = 'morph_' + self.select_column

        extract_morph_words = [' '.join(word) for word in extract_morph_words]

        self.df = pd.DataFrame(extract_morph_words)
        self.df.columns = [self.func_name]
        self.dict[self.func_name] = self.df
        self.dict[self.func_name].to_csv(self.func_name + "_형태소 추출 테스트 결과.csv", index=False)



# if __name__ == "__main__":
#     ps = Preprocessor()
#     # test_df = pd.read_csv("../../전처리 테스트/test_data.csv")
#     keywords = ['경기도', '석촌', '아파트', '부동산', '서울']
#     save_df = ps.clean('mecab', [1,2,3,5], keywords)