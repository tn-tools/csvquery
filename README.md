# csvquery

このプロジェクトはCSVファイルに対してクエリを実現するためのものです。  
簡単に使えて、自由にカスタマイズできます。

## 特徴

- csvファイルに対してjsonファイル設定した内容でクエリができます
- カラムに対してのエイリアス(AS)のサポート
- テーブルに対してのエイリアス(AS)のサポート
- JOINのサポート ※INNER, OUTER, LEFT, RIGHTに対応してます。
- WHEREのサポート ※AND, OR ,() の表現には対応できてません。
- 軽量でシンプルな設計

## インストール方法

```bash
git clone https://github.com/tn-tools/csvquery.git
cd csvquery
pyrhon main.py all
pyrhon main.py tbl1
pyrhon main.py tbl1,tbl2



