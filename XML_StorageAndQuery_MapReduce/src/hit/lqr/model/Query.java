package hit.lqr.model;

public class Query {
	private String queryStatement;

	public String getQueryStatement() {
		return queryStatement;
	}

	public void setQueryStatement(String queryStatement) {
		this.queryStatement = queryStatement;
	}

	public Query(String queryStatement) {
		this.queryStatement = queryStatement;
	}

	public Query() {
		
	}

	@Override
	public String toString() {
		return "Query [queryStatement=" + queryStatement + "]";
	}
	
	
	
	
	
	

}
