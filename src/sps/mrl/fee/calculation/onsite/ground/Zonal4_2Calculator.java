package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal4_2Calculator
 *
 * @author Chilseasai@
 */
public class Zonal4_2Calculator {
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
        return 2.86;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 4.55;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 5.16;
        } else { // shippingWeight > 2.0
            return 5.16 + 0.25 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 5.16;
        } else {
            return 5.16+ 0.25 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 10.58;
        } else {
            return 10.58 + 0.32 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 32.59;
        } else {
            return 32.59 + 0.39 * (shippingWeight - 90.0);
        }
    }
}
