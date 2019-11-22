package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal3_1Calculator
 *
 * @author Chilseasai@
 */
public class Zonal3_1Calculator {

    public double getAmount(final TestCaseItem item) {
        switch (item.getSizeTier()) {
            case "UsSmallStandardSize":
                return calculateSmallStandardSize(item.getShippingWeight());
            case "UsLargeStandardSize":
                return calculateLargeStandardSize(item.getShippingWeight());
            case "UsSmallOversize":
                return calculateSmallOversize(item.getShippingWeight());
            case "UsMediumOversize":
                return calculateMediumOversize(item.getShippingWeight());
            case "UsLargeOversize":
                return calculateLargeOversize(item.getShippingWeight());
            default:
                return 0.0;
        }
    }

    private double calculateSmallStandardSize(final double shippingWeight) {
        return 2.64;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 3.86;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 4.32;
        } else { // shippingWeight > 2.0
            return 4.32 + 0.19 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 4.32;
        } else {
            return 4.32 + 0.19 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 9.75;
        } else {
            return 9.75 + 0.28 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 31.76;
        } else {
            return 31.76 + 0.37 * (shippingWeight - 90.0);
        }
    }
}
