// SPDX-License-Identifier: Apache-2.0
// Copyright (C) 2023 Intel Corporation

// Package frontend implements the FrontEnd APIs (host facing) of the storage Server
package frontend

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/opiproject/gospdk/spdk"
	pb "github.com/opiproject/opi-api/storage/v1alpha1/gen/go"
	"github.com/opiproject/opi-intel-bridge/pkg/models"
	"github.com/opiproject/opi-spdk-bridge/pkg/frontend"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type npiSubsystemListener struct {
}

// NewSubsystemListener creates a new instance of a SubsystemListener for npi transport
func NewSubsystemListener() frontend.SubsystemListener {
	return npiSubsystemListener{}
}

func (c npiSubsystemListener) Params(ctrlr *pb.NvmeController, nqn string) spdk.NvmfSubsystemAddListenerParams {
	result := spdk.NvmfSubsystemAddListenerParams{}
	result.Nqn = nqn
	result.ListenAddress.Trtype = "npi"
	result.ListenAddress.Traddr = calculateTransportAddr(ctrlr.GetSpec().GetPcieId())
	return result
}

func calculateTransportAddr(pci *pb.PciEndpoint) string {
	return strconv.Itoa(int(pci.GetPhysicalFunction())) +
		"." + strconv.Itoa(int(pci.GetVirtualFunction()))
}

// CreateNvmeController creates an Nvme controller
func (s *Server) CreateNvmeController(ctx context.Context, in *pb.CreateNvmeControllerRequest) (*pb.NvmeController, error) {
	log.Printf("Intel bridge CreateNvmeController received from client: %v", in.GetNvmeController())
	if err := s.verifyNvmeControllerOnCreate(in.GetNvmeController()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	log.Printf("Passing request to opi-spdk-bridge")
	response, err := s.FrontendNvmeServiceServer.CreateNvmeController(ctx, in)
	if err == nil {
		// response contains different QoS limits. It is an indication that
		// opi-spdk-bridge returned an already existing controller providing idempotence
		if !proto.Equal(response.GetSpec().GetMaxLimit(), in.GetNvmeController().GetSpec().GetMaxLimit()) ||
			!proto.Equal(response.GetSpec().GetMinLimit(), in.GetNvmeController().GetSpec().GetMinLimit()) {
			log.Printf("Existing NvmeController %v has different QoS limits",
				in.NvmeController)
			return nil, status.Errorf(codes.AlreadyExists,
				"Controller %v exists with different QoS limits", in.GetNvmeController().GetName())
		}

		if qosErr := s.setNvmeQosLimit(in.GetNvmeController()); qosErr != nil {
			s.cleanupNvmeControllerCreation(in.GetNvmeController().GetName())
			return nil, qosErr
		}
	}

	return response, err
}

// UpdateNvmeController updates an Nvme controller
func (s *Server) UpdateNvmeController(ctx context.Context, in *pb.UpdateNvmeControllerRequest) (*pb.NvmeController, error) {
	log.Printf("Intel bridge UpdateNvmeController received from client: %v", in)
	if err := s.verifyNvmeControllerOnUpdate(in.GetNvmeController()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	originalNvmeController := s.nvme.Controllers[in.GetNvmeController().GetName()]
	log.Printf("Passing request to opi-spdk-bridge")
	response, err := s.FrontendNvmeServiceServer.UpdateNvmeController(ctx, in)

	if err == nil {
		if qosErr := s.setNvmeQosLimit(in.GetNvmeController()); qosErr != nil {
			log.Println("Failed to set qos settings:", qosErr)
			log.Println("Restore original controller")
			s.nvme.Controllers[in.GetNvmeController().GetName()] = originalNvmeController
			return nil, qosErr
		}
	}

	return response, err
}

func (s *Server) verifyNvmeControllerOnCreate(controller *pb.NvmeController) error {
	return s.verifyNvmeController(controller)
}

func (s *Server) verifyNvmeControllerOnUpdate(controller *pb.NvmeController) error {
	if err := s.verifyNvmeController(controller); err != nil {
		return err
	}

	// Name had to be assigned on create
	if controller.GetName() == "" {
		return fmt.Errorf("name cannot be empty on update")
	}
	return nil
}

func (s *Server) verifyNvmeController(controller *pb.NvmeController) error {
	maxLimit := controller.GetSpec().GetMaxLimit()
	if err := s.verifyNvmeControllerMaxLimits(maxLimit); err != nil {
		return err
	}

	minLimit := controller.GetSpec().GetMinLimit()
	if err := s.verifyNvmeControllerMinLimits(minLimit); err != nil {
		return err
	}

	return s.verifyNvmeControllerMinMaxLimitCorrespondence(minLimit, maxLimit)
}

func (s *Server) verifyNvmeControllerMaxLimits(maxLimit *pb.QosLimit) error {
	if maxLimit.GetRwIopsKiops() != 0 {
		return fmt.Errorf("QoS max_limit rw_iops_kiops is not supported")
	}
	if maxLimit.GetRwBandwidthMbs() != 0 {
		return fmt.Errorf("QoS max_limit rw_bandwidth_mbs is not supported")
	}

	if maxLimit.GetRdIopsKiops() < 0 {
		return fmt.Errorf("QoS max_limit rd_iops_kiops cannot be negative")
	}
	if maxLimit.GetWrIopsKiops() < 0 {
		return fmt.Errorf("QoS max_limit wr_iops_kiops cannot be negative")
	}
	if maxLimit.GetRdBandwidthMbs() < 0 {
		return fmt.Errorf("QoS max_limit rd_bandwidth_mbs cannot be negative")
	}
	if maxLimit.GetWrBandwidthMbs() < 0 {
		return fmt.Errorf("QoS max_limit wr_bandwidth_mbs cannot be negative")
	}

	return nil
}

func (s *Server) verifyNvmeControllerMinLimits(minLimit *pb.QosLimit) error {
	if minLimit.GetRwIopsKiops() != 0 {
		return fmt.Errorf("QoS min_limit rw_iops_kiops is not supported")
	}
	if minLimit.GetRwBandwidthMbs() != 0 {
		return fmt.Errorf("QoS min_limit rw_bandwidth_mbs is not supported")
	}
	if minLimit.GetRdIopsKiops() != 0 {
		return fmt.Errorf("QoS min_limit rd_iops_kiops is not supported")
	}
	if minLimit.GetWrIopsKiops() != 0 {
		return fmt.Errorf("QoS min_limit wr_iops_kiops is not supported")
	}

	if minLimit.GetRdBandwidthMbs() < 0 {
		return fmt.Errorf("QoS min_limit rd_bandwidth_mbs cannot be negative")
	}
	if minLimit.GetWrBandwidthMbs() < 0 {
		return fmt.Errorf("QoS min_limit wr_bandwidth_mbs cannot be negative")
	}

	return nil
}

func (s *Server) verifyNvmeControllerMinMaxLimitCorrespondence(minLimit *pb.QosLimit, maxLimit *pb.QosLimit) error {
	if maxLimit.GetRdBandwidthMbs() != 0 && minLimit.GetRdBandwidthMbs() > maxLimit.GetRdBandwidthMbs() {
		return fmt.Errorf("QoS min_limit rd_bandwidth_mbs cannot be greater than max_limit rd_bandwidth_mbs")
	}

	if maxLimit.GetWrBandwidthMbs() != 0 && minLimit.GetWrBandwidthMbs() > maxLimit.GetWrBandwidthMbs() {
		return fmt.Errorf("QoS min_limit wr_bandwidth_mbs cannot be greater than max_limit wr_bandwidth_mbs")
	}

	return nil
}

func (s *Server) setNvmeQosLimit(controller *pb.NvmeController) error {
	log.Printf("Setting QoS limits %v for %v", controller.Spec.MaxLimit, controller.Name)
	params := models.NpiQosBwIopsLimitParams{
		Nqn: s.nvme.Subsystems[controller.GetSpec().GetSubsystemNameRef()].GetSpec().GetNqn(),
	}

	maxLimit := controller.GetSpec().GetMaxLimit()
	params.MaxReadIops = int(maxLimit.GetRdIopsKiops())
	params.MaxWriteIops = int(maxLimit.GetWrIopsKiops())
	params.MaxReadBw = int(maxLimit.GetRdBandwidthMbs())
	params.MaxWriteBw = int(maxLimit.GetWrBandwidthMbs())

	minLimit := controller.GetSpec().GetMinLimit()
	params.MinReadBw = int(minLimit.GetRdBandwidthMbs())
	params.MinWriteBw = int(minLimit.GetWrBandwidthMbs())

	var result models.NpiQosBwIopsLimitResult
	err := s.rpc.Call("npi_qos_bw_iops_limit", &params, &result)
	if err != nil {
		log.Printf("error: %v", err)
		return spdk.ErrFailedSpdkCall
	}
	log.Printf("Received from SPDK: %v", result)
	if !result {
		log.Println("Could not set QoS for", controller)
		return spdk.ErrUnexpectedSpdkCallResult
	}
	return nil
}

func (s *Server) cleanupNvmeControllerCreation(id string) {
	log.Println("Cleanup failed Nvme controller creation for", id)
	_, err := s.FrontendNvmeServiceServer.DeleteNvmeController(context.TODO(),
		&pb.DeleteNvmeControllerRequest{Name: id})
	log.Println("Cleanup Nvme controller creation:", err)
}
