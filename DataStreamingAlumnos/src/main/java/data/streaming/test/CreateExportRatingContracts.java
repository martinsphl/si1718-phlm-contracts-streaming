package data.streaming.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

public class CreateExportRatingContracts {

	private Map<String, String> keywordsContracts;
	private ArrayList<String> lstComparationContract = new ArrayList<String>();
	private ArrayList<classificationContracts> cfcContract = new ArrayList<classificationContracts>();

	public CreateExportRatingContracts(Map<String, String> var) {
		keywordsContracts = var;
	}

	public void exportRatingContracts() throws FileNotFoundException {
		PrintWriter pw = new PrintWriter(new File("out/data.csv"));
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cfcContract.size(); i++) {
			sb.append(cfcContract.get(i).getContract1());
			sb.append(',');
			sb.append(cfcContract.get(i).getContract2());
			sb.append(',');
			sb.append(cfcContract.get(i).getR());
			sb.append('\n');
		}
		pw.write(sb.toString());
		pw.close();
		// MongoDB MDB = new MongoDB();
		// MDB.recordClassificationContracts(cfcContract);
	}

	public void analyzeTwo(String contract1, String contract2, String kwC1, String kwC2) {
		String idComparation = contract1 + contract2;

		if (!lstComparationContract.contains(idComparation)) {
			classificationContracts compareTwo = new classificationContracts();
			compareTwo.setContract1(contract1);
			compareTwo.setContract2(contract2);
			compareTwo.setKeywordsC1(kwC1);
			compareTwo.setKeywordsC2(kwC2);
			compareTwo.setR();
			cfcContract.add(compareTwo);
			lstComparationContract.add(contract1 + contract2);
			lstComparationContract.add(contract2 + contract1);
		}

	}

	public void createRatingContracts() throws FileNotFoundException {
		int i = 0;

		classificationContracts contracts = new classificationContracts();
		for (String idContract : keywordsContracts.keySet()) {
			i = i + 1;

			if ((i % 2) == 1) {
				contracts.setContract1(idContract);
				contracts.setKeywordsC1(keywordsContracts.get(idContract));
			} else {
				contracts.setContract2(idContract);
				contracts.setKeywordsC2(keywordsContracts.get(idContract));

				analyzeTwo(contracts.getContract1(), contracts.getContract2(), contracts.getKeywordsC1(),
						contracts.getKeywordsC2());

				for (int j = i; j > 2; j = j - 2) {
					classificationContracts previousContracts = cfcContract.get(((j / 2) - 2));

					analyzeTwo(previousContracts.getContract1(), contracts.getContract1(),
							previousContracts.getKeywordsC1(), contracts.getKeywordsC1());

					analyzeTwo(previousContracts.getContract1(), contracts.getContract2(),
							previousContracts.getKeywordsC1(), contracts.getKeywordsC2());

					analyzeTwo(previousContracts.getContract2(), contracts.getContract1(),
							previousContracts.getKeywordsC2(), contracts.getKeywordsC1());

					analyzeTwo(previousContracts.getContract2(), contracts.getContract2(),
							previousContracts.getKeywordsC2(), contracts.getKeywordsC2());
				}
			}
		}
		exportRatingContracts();
	}
}

/*
 * public void analyzeTwo(String contract1, String contract2, String kwC1,
 * String kwC2) {
 * 
 * if (contract1 != contract2) { String key = contract1 + contract2; if
 * (!lstComparationContract.contains(key)) { classificationContracts compareTwo
 * = new classificationContracts(); compareTwo.setContract1(contract1);
 * compareTwo.setContract2(contract2); compareTwo.setKeywordsC1(kwC1);
 * compareTwo.setKeywordsC2(kwC2); compareTwo.setR();
 * cfcContract.add(compareTwo); lstComparationContract.add(key); key = contract2
 * + contract1; lstComparationContract.add(key); } }
 * 
 * }
 * 
 * public void createRatingContracts() throws FileNotFoundException { int i = 0;
 * 
 * classificationContracts contracts = new classificationContracts(); for
 * (String idContract : keywordsContracts.keySet()) { i = i + 1; if ((i % 2) ==
 * 1) { contracts.setContract1(idContract);
 * contracts.setKeywordsC1(keywordsContracts.get(idContract)); } else {
 * contracts.setContract2(idContract);
 * contracts.setKeywordsC2(keywordsContracts.get(idContract)); contracts.setR();
 * cfcContract.add(contracts);
 * 
 * lstComparationContract.add(contracts.getContract1() +
 * contracts.getContract2());
 * lstComparationContract.add(contracts.getContract2() +
 * contracts.getContract1());
 * 
 * for (int j = 0; j < i; j = j + 2) { classificationContracts previousContracts
 * = cfcContract.get(((j / 2) - 1));
 * 
 * analyzeTwo(previousContracts.getContract1(), contracts.getContract1(),
 * previousContracts.getKeywordsC1(), contracts.getKeywordsC1());
 * 
 * analyzeTwo(previousContracts.getContract1(), contracts.getContract2(),
 * previousContracts.getKeywordsC1(), contracts.getKeywordsC2());
 * 
 * analyzeTwo(previousContracts.getContract2(), contracts.getContract1(),
 * previousContracts.getKeywordsC2(), contracts.getKeywordsC1());
 * 
 * analyzeTwo(previousContracts.getContract2(), contracts.getContract2(),
 * previousContracts.getKeywordsC2(), contracts.getKeywordsC2()); }
 * 
 * } } exportRatingContracts(); } }
 */
