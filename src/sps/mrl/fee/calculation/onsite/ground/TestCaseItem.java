package sps.mrl.fee.calculation.onsite.ground;

import lombok.Data;

import java.io.Serializable;

/**
 * TestCaseItem
 *
 * @author Chilseasai@
 */
@Data
public class TestCaseItem implements Serializable {
    private String Program;
    private String Surcharge;
    private String SizeTier;
    private double ShippingWeight;
    private String ShippingWeightUnit;
    private int ItemQuantity;
    private double Amount;
    private String Currency;
    private String Tax;
    private double ZonalDistribution;
}
