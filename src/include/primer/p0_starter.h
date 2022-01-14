//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {}

  /** The number of rows in the matrix */
  int rows_;
  /** The number of columns in the matrix */
  int cols_;
  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const = 0;

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const = 0;

  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const = 0;

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) = 0;

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) = 0;

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() = default;
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    Matrix<T>::rows_ = rows;
    Matrix<T>::cols_ = cols;
    Matrix<T>::linear_ = new T[Matrix<T>::rows_ * Matrix<T>::cols_]();
    data_ = new T *[Matrix<T>::rows_]();
    for (int i = 0; i < Matrix<T>::rows_; i++) {
      data_[i] = &Matrix<T>::linear_[i * Matrix<T>::cols_];
    }
  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return Matrix<T>::rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return Matrix<T>::cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {
    if (i < 0 || j < 0 || i >= Matrix<T>::rows_ || j >= Matrix<T>::cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "Out of Range");
    }
    return data_[i][j];
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {
    if (i < 0 || j < 0 || i >= Matrix<T>::rows_ || j >= Matrix<T>::cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "Out of Range");
    }
    data_[i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
    if (static_cast<int>(source.size()) != Matrix<T>::rows_ * Matrix<T>::cols_) {
      throw Exception(ExceptionType::OUT_OF_RANGE, "Out of Range");
    }
    for (int i = 0; i < static_cast<int>(source.size()); i++) {
      Matrix<T>::linear_[i] = source[i];
    }
  }
  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
    delete[] data_;
    delete[] Matrix<T>::linear_;
  }

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T **data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    int ca = matrixA->RowMatrix<T>::GetColumnCount();
    int ra = matrixA->RowMatrix<T>::GetRowCount();
    int cb = matrixB->RowMatrix<T>::GetColumnCount();
    int rb = matrixB->RowMatrix<T>::GetRowCount();
    if (ca == cb && ra == rb) {
      auto matrix = std::make_unique<RowMatrix<T>>(ra, ca);
      for (int i = 0; i < ra; i++) {
        for (int j = 0; j < ca; j++) {
          int va = matrixA->RowMatrix<T>::GetElement(i, j);
          int vb = matrixB->RowMatrix<T>::GetElement(i, j);
          matrix->RowMatrix<T>::SetElement(i, j, va + vb);
        }
      }
      return matrix;
    }
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation
    int ca = matrixA->RowMatrix<T>::GetColumnCount();
    int ra = matrixA->RowMatrix<T>::GetRowCount();
    int cb = matrixB->RowMatrix<T>::GetColumnCount();
    int rb = matrixB->RowMatrix<T>::GetRowCount();

    if (ca == rb) {
      auto matrix = std::make_unique<RowMatrix<T>>(ra, cb);
      for (int i = 0; i < ra; i++) {
        for (int j = 0; j < cb; j++) {
          int sum = 0;
          for (int m = 0; m < ca; m++) {
            sum += (matrixA->RowMatrix<T>::GetElement(i, m) * matrixB->RowMatrix<T>::GetElement(m, j));
          }
          matrix->RowMatrix<T>::SetElement(i, j, sum);
        }
      }
      return matrix;
    }
    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    int ca = matrixA->RowMatrix<T>::GetColumnCount();
    int ra = matrixA->RowMatrix<T>::GetRowCount();
    int cb = matrixB->RowMatrix<T>::GetColumnCount();
    int rb = matrixB->RowMatrix<T>::GetRowCount();
    int cc = matrixC->RowMatrix<T>::GetColumnCount();
    int rc = matrixC->RowMatrix<T>::GetRowCount();

    if (ca == rb && ra == rc && cb == cc) {
      auto matrix = std::make_unique<RowMatrix<T>>(rc, cc);
      for (int i = 0; i < ra; i++) {
        for (int j = 0; j < cb; j++) {
          int sum = 0;
          for (int m = 0; m < ca; m++) {
            sum += (matrixA->RowMatrix<T>::GetElement(i, m) * matrixB->RowMatrix<T>::GetElement(m, j));
          }
          matrix->RowMatrix<T>::SetElement(i, j, sum);
        }
      }

      for (int i = 0; i < rc; i++) {
        for (int j = 0; j < cc; j++) {
          int tmp = matrix->RowMatrix<T>::GetElement(i, j);
          matrix->RowMatrix<T>::SetElement(i, j, tmp + matrixC->RowMatrix<T>::GetElement(i, j));
        }
      }
      return matrix;
    }

    return std::unique_ptr<RowMatrix<T>>(nullptr);
  }
};
}  // namespace bustub
