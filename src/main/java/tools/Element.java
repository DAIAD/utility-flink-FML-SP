package tools;

import java.io.Serializable;


public class Element implements Serializable {

	private static final long serialVersionUID = 1L;
	public String id;
	private Double[] values;

	public Element() {
	}

	public Element(String id, String input) {
		input = input.replaceAll("\\s+", " ");
		String[] parts = input.split(" ");
		this.values = new Double[parts.length];
		for (int i = 0; i < parts.length; i++) {
			this.values[i] = Double.parseDouble(parts[i]);
		}
		this.id = id;
	}

	public Element(String id, Double[] values) {
		this.id = id;
		this.values = values;
	}

	public Element add(Element other) {
		for (int i = 0; i < this.values.length; i++) {
			this.values[i] += other.values[i];
		}
		return this;
	}

	public Double[] div(long val) {
		for (int i = 0; i < this.values.length; i++) {
			this.values[i] /= val;
		}
		return this.values;
	}

	public void clear() {
		for (int i = 0; i < values.length; i++) {
			values[i] = 0.0;
		}
	}

	public Double[] getValues() {
		return values;
	}

	public void setValues(Double[] values) {
		this.values = values;
	}

	@Override
	public String toString() {
		String str = this.id + ",";
		for (int i = 0; i < this.values.length; i++) {
			//str += (this.values[i] + " ");
		}
		return str;
	}

}
