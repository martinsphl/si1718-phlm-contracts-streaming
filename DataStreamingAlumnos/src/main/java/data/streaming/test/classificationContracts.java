package data.streaming.test;

import it.unimi.dsi.fastutil.Arrays;

public class classificationContracts {

	private String contract1, contract2, keywordsC1, keywordsC2;
	private float r;
	private float sameKeywords;

	public String toString() {
		return contract1 + ", " + contract2 + ", " + r;
	}

	public String getContract2() {
		return contract2;
	}

	public void setContract2(String contract2) {
		this.contract2 = contract2;
	}

	public String getContract1() {
		return contract1;
	}

	public void setContract1(String contract1) {
		this.contract1 = contract1;
	}

	public float getR() {
		return r;
	}

	public void setR() {
		r = 0;
		sameKeywords = 0;
		String[] kC1 = keywordsC1.replace("[", "").replace("]", "").replace(":", "").split(", ");
		String[] kC2 = keywordsC2.replace("[", "").replace("]", "").replace(":", "").split(", ");

		if (kC1.length == 0 && kC2.length == 0) {
			r = 0;
			return;
		}

		for (int i = 0; i < kC2.length; i++) {
				if (java.util.Arrays.asList(kC1).contains(kC2[i]) && contract1.compareToIgnoreCase(contract2) != 0) {
					sameKeywords = sameKeywords + 1;
				}
		}
		
		if (kC1.length < kC2.length)
			r = Math.round((5 * sameKeywords) / kC2.length);
		else
			r = Math.round((5 * sameKeywords) / kC1.length);
	}

	public String getKeywordsC1() {
		return keywordsC1;
	}

	public void setKeywordsC1(String keywordsC1) {
		this.keywordsC1 = keywordsC1;
	}

	public String getKeywordsC2() {
		return keywordsC2;
	}

	public void setKeywordsC2(String keywordsC2) {
		this.keywordsC2 = keywordsC2;
	}
}