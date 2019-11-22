package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal4_1Calculator
 *
 * @author Chilseasai@
 */
public class Zonal4_1Calculator {
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
        return 2.85;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 4.49;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 5.09;
        } else { // shippingWeight > 2.0
            return 5.09 + 0.24 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 5.09;
        } else {
            return 5.09 + 0.24 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 10.52;
        } else {
            return 10.52 + 0.32 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 32.53;
        } else {
            return 32.53 + 0.38 * (shippingWeight - 90.0);
        }
    }
}
