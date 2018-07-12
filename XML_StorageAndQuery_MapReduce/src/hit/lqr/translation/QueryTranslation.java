package hit.lqr.translation;

import java.util.ArrayList;
import hit.lqr.model.Query;
/**
 * 
 * @author LiuQIuru
 * 将Xpath语句翻译为系统能够处理的简单路径表达式集合
 *
 */
public class QueryTranslation {
	/*private Query query;

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	} */
	
	public ArrayList<String> translate(String queryStatement) {
		ArrayList<String> result = new ArrayList<String>();
		if(queryStatement == null || queryStatement.length() == 0) {
			return result;
		}
		
		String statement;
		if(!queryStatement.contains("[")) {
			System.out.println("此查询语句为简单查询语句");
			//简单语句处理
		}else {
			System.out.println("此查询语句为复杂查询语句");
			String[] splitStr = queryStatement.split("[\\[\\]]");                                                                       //分割字符串有多个分隔符的可以用正则表达式的形式[],如分隔符为，和&，则为spilt("[,&]")，而分隔符为[及]时，由于有特殊含义，应在前面加\\
            //带谓词的xpath语句按”[“及”]“进行拆分，拆分后非谓词以”/"开头，而谓词不是
			int valFlag = 0;                                                                                                                //定义分解得到语句的类型的标志，共3种，0为无用的语句，reduce处理时直接忽略；1为条件语句；2为实际查询语句。
			int count = 0,icount = 1; 
			int conditionFlag = 1;
			String commonStr = splitStr[0];                                                                               //语句中的公共节点或公共“链”，初始为拆分字符串数组的0号单元的值
			
			String basicVal = splitStr[0];                                                                                    //定义一个哨兵字符串，初始为拆分字符串数组的0号单元的值
			String val = splitStr[0];
			statement = valFlag + "&" + val;
			System.out.println(statement);
			result.add(statement);
			
			for(int i = 0;i<splitStr.length;i++) {                                                                           //统计给出的查询语句中的非谓词部分，用于确定实际查询语句
				if(splitStr[i].charAt(0) == '/') {
					count++;
				}
			}
			
			if(icount == count) {                                                                                           //实际查询语句为最后更新的哨兵字符串,该种情况对应于类似//book[name='Fan'],即非谓词部分只有一个的情况
				valFlag = 2;
				statement = valFlag + "&" + val;
				System.out.println(statement);
				result.add(statement);
			}
			
			
			//扫描该字符串数组，若是谓词，则路径表达式为哨兵字符串与"/"及该单元处理后的值的连接；若不是谓词，则更新哨兵字符串为之前的哨兵字符串与该单元值的连接，路径表达式为更新后的哨兵表达式
			for(int i = 1;i<splitStr.length;i++) {
				//System.out.println(splitStr[i]);
				if(splitStr[i].charAt(0) != '/') {
					//splitPre = splitStr[i].split("[><=!]");                                                           //将谓词进行拆分，去掉比较符及其后面的内容
					//val = basicVal + "/" + splitPre[0];
					val = basicVal + "/" + splitStr[i];
					valFlag = 1;
					statement = valFlag + "&" + conditionFlag + "&" + val + "&" + commonStr;
					conditionFlag ++;
					System.out.println(statement);
					result.add(statement);
				}else {
					valFlag = 0;
					basicVal = basicVal + splitStr[i];
					val = basicVal;
					commonStr = splitStr[i];                                                                                     //语句的公共节点或公共链为每次哨兵字符串新加的部分
					icount++;
					if(icount == count) {                                                                                           //实际查询语句为最后更新的哨兵字符串
						valFlag = 2;
					}
				statement = valFlag + "&" + val;
				System.out.println(statement);
				result.add(statement);
			}
		}		
	}
		return result;
	}
	
	/*public static void main(String[] args) {
		String s = "/node/buffer[hi='nihao']//haha[@love='3']";
		ArrayList<String> result = new QueryTranslation().translate(s);
		System.out.println(result);
	}*/

}
