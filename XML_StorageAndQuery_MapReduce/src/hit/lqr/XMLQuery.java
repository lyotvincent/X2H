package hit.lqr;

import java.util.ArrayList;

import hit.lqr.service.query.QueryWithMR;
import hit.lqr.translation.QueryTranslation;

/**
 * 
 * @author LiuQiuru
 * 基于MR的XML文档查询主函数
 *
 */
public class XMLQuery {

	
	public static void main(String[] args) {
		/*long startTime = System.currentTimeMillis();
		String queryStatement = "//teachers[teacher/@id='001']";
		QueryTranslation qt = new QueryTranslation();
		ArrayList<String> translationSet = qt.translate(queryStatement);
		System.out.println("***************before*******************");
	    System.out.println(translationSet);
		QueryWithMR queryMR = new QueryWithMR(translationSet);*/
		QueryWithMR queryMR = new QueryWithMR();
		//queryMR.setTranslationSet(translationSet);
		queryMR.xmlQuery();

		//long endTime = System.currentTimeMillis();
	    //System.out.println("程序运行的时间是1：" +  (endTime - startTime) + "ms");
	}

}
