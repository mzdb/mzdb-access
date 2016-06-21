package fr.profi.mzdb.model;

/**
 * @author David Bouyssie
 *
 */
public interface IPeak {

  /**
   * Gets the mz.
   * 
   * @return the mz
   */
  double getMz();

  /**
   * Gets the intensity.
   * 
   * @return the intensity
   */
  float getIntensity();

  /**
   * Gets the left hwhm.
   * 
   * @return the left hwhm
   */
  float getLeftHwhm();

  /**
   * Gets the right hwhm.
   * 
   * @return the right hwhm
   */
  float getRightHwhm();

  /**
   * Gets the lc context.
   * 
   * @return the lc context
   */
  ILcContext getLcContext();

}